/**
 * app.js
 */
const util = require('util');
const tools = require('./tools');
const Client = require('./client');

module.exports = async function (plugin) {
  const clientArr = [];
  const params = plugin.params;
  const connections = 1;
  let channels = [];
  let polls = [];
  let maxreadtags = 0;
  try {
    channels = await plugin.channels.get();
    plugin.log('Received channels data: ' + util.inspect(channels), 2);
    await updateChannels(false);
  } catch (err) {
    plugin.exit(2, 'Failed to get channels!');
  }
  plugin.channels.onChange(() => updateChannels(true));

  // TODO - нужно делить каналы по клиентам
  // пока все клиенты читают все каналы
  // Также непонятно, как быть при записи, можно
  // - писать в одном канале (firstClient)
  // - слушать здесь: plugin.onAct, затем писать в  qToWrite конкретного клиента
  // - перенести plugin.onAct в клиента, каждый клиент будет сам определять, его ли это адрес

  if (polls.length > connections) {
    maxreadtags = Math.ceil(polls.length / connections);
  } else {
    maxreadtags = polls.length;
  }
  //plugin.log('maxreadtags ' + maxreadtags, 1);
  try {
    clientArr[0] = new Client(plugin, params, 0);
    await clientArr[0].connect();
    clientArr[0].setPolls(polls);
    clientArr[0].sendNext();
  } catch (e) {
    plugin.log("Error" + e, 1)
  }
  /*let i = 0;
   while (polls.length > 0) {
     try {
       let nextClient = new Client(plugin, params, i);
       clientArr[i] = nextClient;
       await nextClient.connect();
       let chunk = polls.splice(0, maxreadtags);
       nextClient.setPolls(chunk);
       nextClient.sendNext();
       i++;
     } catch (e) {
       plugin.log('Client ' + i + ' error: ' + util.inspect(e), 1);
       plugin.exit(1, "No connection")
     }
   }*/

  function terminatePlugin() {
    for (let i = 0; i < clientArr.length; i++) {
      if (clientArr[i] && clientArr[i].isOpen) clientArr[i].close();
    }
  }

  plugin.onCommand(message => {
    plugin.log('Get command ' + util.inspect(message), 1);
    if (message.command == 'writereadOnReq') {
      message.values.forEach(item => {
        item.address = Number(item.address);
        item.vartype += 'be';
      });
      clientArr[0].setWriteRead(message);
    }
    if (message.command == 'readOnReq') {
      message.values = channels;
      clientArr[0].setRead(message);
    }
  });

  plugin.onAct(message => {
    plugin.log('ACT data=' + util.inspect(message.data), 1);
    // TODO - нужно распределить по клиентам??
    clientArr[0].setWrite(message.data);
  });

  async function updateChannels(getChannels) {
    // TODO - при обновлении в работе??
    // if (this.queue !== undefined) {
    //  await this.sendNext(true);
    // }

    if (getChannels === true) {
      plugin.log('Request updated channels', 2);
      channels = await plugin.channels.get();
      channels.forEach(item => {
        item.address = parseInt(item.address);
        if (item.parentoffset) item.address += parseInt(item.parentoffset);
        item.vartype = item.manbo ? tools.getVartypeMan(item) : tools.getVartype(item.vartype, params);
      });
      polls = tools.getPolls(
        channels.filter(item => item.r),
        params
      );

      if (polls.length > connections) {
        maxreadtags = Math.ceil(polls.length / connections);
      } else {
        maxreadtags = polls.length;
      }
      plugin.log('maxreadtags ' + polls.length, 1);
      for (let i = 0; i < clientArr.length; i++) {
        try {
          nextClient = clientArr[i];
          let chunk = polls.splice(0, maxreadtags);
          nextClient.setPolls(chunk);
        } catch (e) {
          plugin.log('Client ' + i + ' error: ' + util.inspect(e), 1);
        }
      }
      return;
    }

    if (channels.length === 0) {
      plugin.log(`Channels do not exist!`, 2);
      process.exit(8);
    }

    channels.forEach(item => {
      if (!item.adrclass) {
        plugin.log(
          'Channel ERROR: Unknown adrclass:' +
          item.adrclass +
          ' address:' +
          item.address +
          ' title:' +
          item.chan +
          '.  Skipped..', 1
        );
        item.r = 0;
      } else {
        item.address = parseInt(item.address);
        if (item.parentoffset) item.address += parseInt(item.parentoffset);
        item.vartype = item.manbo ? tools.getVartypeMan(item) : tools.getVartype(item.vartype, params);
      }
    });

    polls = tools.getPolls(
      channels.filter(item => item.r),
      params
    );
    //plugin.log(`Polls = ${util.inspect(polls, null, 4)}`, 2);

    // TODO - передать новый polls клиентам??

    // this.queue = tools.getPollArray(this.polls); // Очередь опроса -на чтение
    // this.qToWrite = []; // Очередь на запись - имеет более высокий приоритет
    // this.sendTime = 0;
  }

  process.on('exit', terminatePlugin);
  process.on('SIGTERM', () => {
    process.exit(0);
  });
};
