/**
 * client.js
 */

const util = require('util');
const fins = require('omron-fins');

const tools = require('./tools');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

class Client {
  constructor(plugin, params, idx) {
    this.plugin = plugin;
    this.params = params;
    this.idx = idx;
    this.options = {};
    this.isOpen = false;
    this.conn = {};

    this.polls = [];
    this.queue = [];
    this.channelsChstatus = {};
    this.channelsData = {};
    this.qToWrite = []; // Очередь на запись 
    this.qToWriteRead = [];
    this.qToRead = [];
    this.message = {};
  }

  async connect() {
    const host = this.params.host;
    const port = Number(this.params.port);
    
    const transport = this.params.transport || 'tcp';
    const timeout = this.params.timeout;
    const DNA = this.params.DNA || 0;
    const DA1 = this.params.DA1 || 0;
    const DA2 = this.params.DA2 || 0;
    const SNA = this.params.SNA || 0;
    const SA1 = this.params.SA1 || 0;
    const SA2 = this.params.SA2 || 0;
    
   if (this.params.advConnectionParams == 1) {      
      this.options = { timeout, max_queue: 2, protocol: transport, DNA, DA1, DA2, SNA, SA1, SA2 }
    } else {
      this.options = { timeout, max_queue: 2, protocol: transport }
    }
    this.clientLog('Try connect to ' + host + ':' + port + " with options " + util.inspect(this.options));
    this.conn = fins.FinsClient(port, host, this.options, true);
    return new Promise((resolve, reject) => {
      const self = this;
      this.conn.on('open', function (info) {
        self.isOpen = true;
        self.clientLog('Connected ' + host + ':' + port);
        resolve(info);
      });
      this.conn.on('close', function () {
        self.isOpen = false;
        let channelsStatus = [];
        self.polls.forEach(item => {
          item.ref.forEach(item1 => {
            channelsStatus.push({ id: item1.id, chstatus: 1, title: item1.title })
          })
        })
        self.plugin.sendData(channelsStatus);
        self.plugin.exit(1, 'Client ' + self.idx + ' disconnected');
      });

      this.conn.on('error', e => {
        self.isOpen = false;
        self.clientLog('Error ' + e);
        reject("error " + e);
      });

      this.conn.on('timeout', function (host, msg) {
        reject(host + " timeout");
      });

    });
  }


  setPolls(polls) {
    this.polls = polls;
    this.channelsData = {};
  }

  setWriteRead(message) {
    this.qToWriteRead = tools.getWriteRequests(message.values, this.params);
    this.message = { unit: message.unit, param: message.param, sender: message.sender, type: message.type, uuid: message.uuid };
    // this.plugin.log('channels: ' + util.inspect(this.qToWriteRead), 2);
  }

  setRead(message) {
    this.qToRead = tools.getRequests(message.values, this.params);
    this.message = { unit: message.unit, param: message.param, sender: message.sender, type: message.type, uuid: message.uuid };
    // this.clientLog('message ' + util.inspect(this.qToRead), 2);
  }

  setWrite(data) {
    try {
      data.forEach(aitem => {
        if (aitem) {
          this.plugin.log('chanItem: ' + util.inspect(aitem), 2);
          const item = tools.formWriteObject(aitem, this.params);
          if (item && item.vartype) {
            this.qToWrite.push(item);
            this.plugin.log(`Command to write: ${util.inspect(this.qToWrite)}`, 2);
          } else {
            this.plugin.log('ERROR: Command has empty vartype: ' + util.inspect(item), 2);
          }
        }
      });
    } catch (err) {
      this.checkError(err);
    }
  }

  async sendNext(single) {
    // this.clientLog('sendNext');
    if (!this.isOpen) {
      let channelsStatus = [];
      this.polls.forEach(item => {
        item.ref.forEach(item1 => {
          channelsStatus.push({ id: item1.id, chstatus: 1, title: item1.title })
        })
      })
      this.plugin.sendData(channelsStatus);
      this.plugin.exit(1, 'Port not open!!!!');
    }

    let isOnce = false;
    if (typeof single !== undefined && single === true) {
      isOnce = true;
    }

    let item;
    if (this.qToWriteRead.length) {
      item = this.qToWriteRead.shift();
      return this.writeReadRequest(item, !isOnce);
    }

    if (this.qToWrite.length) {
      item = this.qToWrite.shift();
      return this.write(item, !isOnce);
    }

    if (this.qToRead.length) {
      item = this.qToRead.shift();
      return this.readRequest(item, !isOnce);
    }


    if (this.queue.length <= 0) {
      this.polls.forEach(pitem => {
        if (pitem.curpoll < pitem.polltimefctr) {
          pitem.curpoll++;
        } else {
          pitem.curpoll = 1;
        }
      });
      this.queue = tools.getPollArray(this.polls);
    }

    item = this.queue.shift();
    if (typeof item !== 'object') {
      item = this.polls[item];
    }

    if (item) {
      return this.read(item, !isOnce);
    }

    await sleep(this.params.polldelay || 1);
    setImmediate(() => {
      this.sendNext();
    });
  }

  async read(item, allowSendNext) {
    this.clientLog(
      `READ: Class = ${item.adrclass}, address = ${tools.showAddress(item.address)}, length = ${item.length}`,
      1
    );

    try {
      let res = await this.readCommand(item.adrclass, item.address, item.length, item.ref);
      if (res && res.buffer) {
        const data = tools.getDataFromResponse(res.buffer, item.ref);
        /*data.forEach(el => {
          if (isNaN(el.value)) {
            el.chstatus = 1;
          }
          this.channelsChstatus[el.id] = el.chstatus;
        });*/

        if (this.params.sendChanges == 1) {
          let arr = data.filter(ditem => {
            if (this.channelsData[ditem.id] != ditem.value) {
              this.channelsData[ditem.id] = ditem.value;
              return true;
            }
            return false;
          });
          if (arr.length > 0) this.plugin.sendData(arr);
        } else {
          this.plugin.sendData(data);
        }

        // this.plugin.log(res.buffer, 2);
      }
    } catch (err) {
      this.checkError(err);
    }

    if (this.qToWrite.length || allowSendNext) {
      if (!this.qToWrite.length) {
        await sleep(this.params.polldelay || 1); // Интервал между запросами
      }
      setImmediate(() => {
        this.sendNext();
      });
    }
  }

  async readCommand(adrclass, address, length, ref) {
    try {
      return await this.readRegisters(adrclass, address, length);
    } catch (err) {
      let charr = ref.map(item => ({ id: item.id, chstatus: 1, title: item.title }));
      charr.forEach(el => {
        this.channelsChstatus[el.id] = 1;
      });
      this.plugin.sendData(charr);
      this.checkError(err);
    }
  }

  checkError(e) {
    // TODO - выход пока заглушен!
    let exitCode = 0;
    if (e.errno && networkErrors.includes(e.errno)) {
      this.clientLog(`Network ERROR: ${e.errno}`, 1);
      exitCode = 1;
      this.plugin.exit(exitCode, util.inspect(e));
    }
    if (e.message.includes("Timeout")) {
      exitCode = 2;
      this.plugin.exit(exitCode, util.inspect(e));
    }
    else {
      this.clientLog('ERROR: ' + util.inspect(e), 1);

      // TODO Если все каналы c chstatus=1 - перезагрузить плагин?
      /*
      for (const item of this.channels) {
        if (!this.channelsChstatus[item.id]) return;
      }
      */
      this.clientLog('All channels have bad status! Exit with code 42', 1);
      exitCode = 42;
    }
    //this.plugin.exit(exitCode, util.inspect(e));
    //process.exit(exitCode);
    return exitCode;
  }

  readRegisters(adrclass, address, length) {
    return new Promise((resolve, reject) => {
      let addr = adrclass + address;

      this.conn.read(addr, length, null);
      const timerId = setTimeout(resTimeout, this.params.timeout);
      let self = this.plugin;
      this.conn.on('reply', function getData(data) {
        clearTimeout(timerId);
        this.removeListener('reply', getData);
        resolve({ buffer: data.response.buffer });
      });
      function resTimeout() {
        this.isOpen = false;
        reject({ message: 'Timeout: ' + this.params.timeout })
      }
    });
  }

  async writeReadRequest(item, allowSendNext) {
    const mismatched = [];
    try {
      const resWrite = await this.writeRegisters(item.adrclass, item.address, item.bufdata);
      const resRead = await this.readRegisters(item.adrclass, item.address, item.length);
      const dataRead = tools.getCorrectDataFromResponse(resRead.buffer, item.ref);

      dataRead.forEach(ritem => {
        if (ritem.value != ritem.writeValue) {
          this.qToWriteRead = [];
          mismatched.push(`${item.title} writeValue = ${ritem.writeValue} readValue = ${ritem.value}`);
        }
      });
      this.plugin.sendData(dataRead);
      // this.clientLog(`Read result: ${util.inspect(resRead)}`, 1);
    } catch (err) {
      this.checkError(err);
    }

    if (!this.qToWriteRead.length) {
      if (mismatched.length) {
        this.message.result = "Write/Read mismatch, items: " + mismatched.join('; ');
        this.plugin.sendResponse(this.message, 0);
      } else {
        this.message.result = "Write/Read Ok";
        this.plugin.sendResponse(this.message, 1);
      }
    }

    if (this.qToWriteRead.length || allowSendNext) {
      if (!this.qToWriteRead.length) {
        await sleep(this.params.polldelay || 10); // Интервал между запросами
      }

      setImmediate(() => {
        this.sendNext();
      });
    }
  }

  async readRequest(item, allowSendNext) {
    try {
      const res = await this.readRegisters(item.adrclass, item.address, item.length);
      if (res && res.buffer) {

        const data = tools.getDataFromResponse(res.buffer, item.ref);
        this.plugin.sendData(data);
      }
    } catch (error) {
      this.message.result = "Read Fail";
      this.plugin.sendResponse(this.message, 1);
      this.checkError(error);
    }

    if (this.qToRead.length == 0) {
      this.message.result = "Read Ok";
      this.plugin.sendResponse(this.message, 1);
    }


    if (this.qToRead.length || allowSendNext) {
      if (!this.qToRead.length) {
        await sleep(this.params.polldelay || 10); // Интервал между запросами
      }

      setImmediate(() => {
        this.sendNext();
      });
    }
  }

  async write(item, allowSendNext) {
    let val = [];
    let adr;
    if (item.offset != undefined) {
      this.clientLog(
        `writeDiscretes: address = ${tools.showAddress(item.address)}, offset = ${item.offset} value = ${item.value}`,
        1
      );
      adr = item.address + "." + item.offset;
      val = item.value;
    } else {
      adr = item.address;
      const buf = tools.writeValue(item.value, item);
      for (let i = 0; i < buf.length; i = i + 2) {
        val.push(buf.readUInt16BE(i));
      }
    }

    this.clientLog(
      `WRITE: Class = ${item.adrclass}, address = ${tools.showAddress(item.address)}, value = ${util.inspect(val)}`,
      1
    );

    // Результат на запись - принять!!
    try {
      let res = await this.writeCommand(item.adrclass, adr, val);
      //this.plugin.sendData([{ id: item.id, value: val }]);
      // Получили ответ при записи
      this.clientLog(`Write result: ${util.inspect(res)}`, 1);

      if (item.force) {
        let res = await this.readRegisters(item.adrclass, resWrite.address, resWrite.length);
        item.widx = 0;
        let value = tools.readValue(res.buffer, item);
        this.clientLog(`Read result: ${util.inspect(value)}`, 1);
        // Если канал получает данные по запросу, то отправлять сразу после записи
        this.plugin.sendData([{ id: item.id, value: value }]);
      }
    } catch (err) {
      this.checkError(err);
    }

    if (this.qToWrite.length || allowSendNext) {
      if (!this.qToWrite.length) {
        await sleep(this.params.polldelay || 1); // Интервал между запросами
      }
      setImmediate(() => {
        this.sendNext();
      });
    }
  }

  async writeCommand(adrclass, address, value) {
    try {
      this.clientLog(`writeRegisters: address = ${tools.showAddress(address)}, value = ${util.inspect(value)}`, 1);
      return await this.writeRegisters(adrclass, address, value);
    } catch (err) {
      this.checkError(err);
    }
  }

  writeRegisters(adrclass, address, value) {
    return new Promise((resolve, reject) => {
      let addr = adrclass + address;

      this.conn.write(addr, value);
      const timerId = setTimeout(resTimeout, this.params.timeout);
      let self = this.plugin;
      this.conn.on('reply', function getData(data) {
        clearTimeout(timerId);
        this.removeListener('reply', getData);
        if (data.response.endCode == '0000') {
          resolve("Write Ok");
        } else {
          reject("Write Error code: " + data.response.endCode)
        }
        
      });
      function resTimeout() {
        this.isOpen = false;
        reject('Timeout: ' + this.params.timeout)
      }
    });
  }

  close() {
    return new Promise((resolve, reject) => {
      this.conn.disconnect(err => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  clientLog(txt, level = 0) {
    this.plugin.log('Client ' + this.idx + ' ' + txt, level, 1)
  }
}
module.exports = Client;
