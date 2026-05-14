const express = require('express');
const bodyParser = require('body-parser');
// **NOTE: ws3-fca is an older/forked version. Using 'fca-unofficial' is often more stable, 
// but sticking to 'ws3-fca' as requested by the original code.**
const login = require('ws3-fca'); 
const fs = require('fs');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// === GLOBAL STATE ===
let botAPI = null;
let adminID = null;
let prefix = '/';
let botNickname = 'LEGEND PRINCE';
let lockedGroups = {};       // threadID -> title
let lockedNicknames = {};    // threadID -> nickname
let lockedTargets = {};      // threadID -> targetUserID[]
let autoAddUsers = {};       // threadID -> { [userID]: true }  
let currentCookies = null;
let reconnectAttempt = 0;
let conversationState = {}; // threadID -> stage

// Track last message to avoid spam replies
let lastMessageTime = {}; // threadID -> timestamp

const signature = `\n\n— 💕𝑴𝑹 𝑷𝑹𝑰𝑵𝑪𝑬 💕`;
const separator = `\n------------------------------`;

// === MASTI AUTO REPLY (UNCHANGED) ===
const mastiReplies = [
  ""
];

// === LOG SYSTEM (IMPROVED) ===
function emitLog(message, isError = false) {
  const logMessage = `[${new Date().toISOString()}] ${isError ? 'ERROR: ' : 'INFO: '}${message}`;
  console.log(logMessage);
  io.emit('botlog', logMessage);
  // Log critical errors to a file (optional, but good for diagnostics)
  if (isError) {
      fs.appendFileSync('critical_errors.log', logMessage + '\n');
  }
}

function saveConfig() {
  try {
    // Only save cookies if they exist and are an array
    const cookiesToSave = (Array.isArray(currentCookies) && currentCookies.length) ? currentCookies : null;
    const toSave = {
      botNickname,
      cookies: cookiesToSave,
      adminID,
      prefix,
      lockedGroups,
      lockedNicknames,
      lockedTargets,
      autoAddUsers
    };
    fs.writeFileSync('config.json', JSON.stringify(toSave, null, 2));
    emitLog('Configuration saved successfully.');
  } catch (e) {
    emitLog('🚨 Failed to save config: ' + e.message, true);
  }
}

// === BOT INIT ===
function initializeBot(cookies, prefixArg, adminArg) {
  // Gracefully stop old listening instance if it exists
  if (botAPI && botAPI.stopListening) {
      try { botAPI.stopListening(); } catch (e) { emitLog('Error stopping previous listener: ' + e.message, true); }
  }
  botAPI = null; // Clear the old API instance

  emitLog('🚀 Initializing bot login...');
  currentCookies = cookies;
  if (prefixArg) prefix = prefixArg;
  if (adminArg) adminID = adminArg;
  reconnectAttempt = 0;

  login({ appState: currentCookies }, (err, api) => {
    if (err) {
      // **CRITICAL ERROR HANDLING FOR APPSTATE EXPIRED/INVALID**
      let errorMessage = err.message || 'Unknown login error.';
      if (errorMessage.includes('Invalid appstate') || errorMessage.includes('Logged in as') && errorMessage.includes('user')) {
          emitLog('🚫 AppState/Cookies Expired or Invalid! Please update cookies in dashboard.', true);
          // Do not attempt automatic reconnect on permanent AppState failure
          return; 
      }
      
      emitLog(`⚠️ Login error: ${errorMessage}. Retrying in 10s.`, true);
      setTimeout(() => initializeBot(currentCookies, prefix, adminID), 10000);
      return;
    }

    emitLog('✅ Bot logged in successfully. Starting listeners...');
    botAPI = api;
    botAPI.setOptions({ selfListen: true, listenEvents: true, updatePresence: false });

    // Save fresh cookies received after successful login
    currentCookies = api.getAppState(); 
    saveConfig(); 

    setTimeout(async () => {
      try { await setBotNicknamesInGroups(); } catch (e) { emitLog('Error restoring nicknames: ' + e.message, true); }
      startListening(api);
    }, 2000);

    // Ensure saveConfig runs periodically (5 minutes)
    setInterval(saveConfig, 5 * 60 * 1000);
  });
}

// === RECONNECT SYSTEM (IMPROVED) ===
function reconnectAndListen() {
  reconnectAttempt++;
  emitLog(`🔄 Reconnect attempt #${reconnectAttempt}...`);
  
  // If API object exists, stop listening gracefully first
  if (botAPI && botAPI.stopListening) {
    try { botAPI.stopListening(); } catch {}
  }
  
  if (reconnectAttempt > 10) { // Increased attempts for minor connection issues
    emitLog('🔴 Max reconnect attempts reached; Reinitializing full login process.', true);
    initializeBot(currentCookies, prefix, adminID); // Full re-login attempt
  } else {
    // Only attempt to restart listener if botAPI is valid (Appstate is not expired)
    setTimeout(() => {
      if (botAPI && botAPI.listenMqtt) startListening(botAPI); 
      else initializeBot(currentCookies, prefix, adminID); // Fallback to full login
    }, 5000);
  }
}

// === LISTENER ===
function startListening(api) {
  // Ensure we don't start multiple listeners
  if (api.__isListening) return emitLog('Listener already active, skipping.');
  api.__isListening = true; 

  api.listenMqtt(async (err, event) => {
    // ⚠️ CRITICAL: Handle listener errors (connection drops)
    if (err) {
      api.__isListening = false;
      emitLog('❌ Listener MQTT error: ' + err.message, true);
      reconnectAndListen();
      return;
    }
    
    // Reset reconnect attempts on successful event
    reconnectAttempt = 0; 

    try {
      if (event.type === 'message' || event.type === 'message_reply') {
        await handleMessage(api, event);
      } else if (event.logMessageType === 'log:thread-name') {
        await handleThreadNameChange(api, event);
      } else if (event.logMessageType === 'log:user-nickname') {
        await handleNicknameChange(api, event);
      } else if (event.logMessageType === 'log:subscribe') {
        await handleBotAddedToGroup(api, event);
      } else if (event.logMessageType === 'log:unsubscribe') {
        await handleUserLeftGroup(api, event);
      }
    } catch (e) {
      // Handle errors within event handlers without crashing the listener loop
      emitLog('⚠️ Handler crashed for event type ' + event.type + ': ' + e.message, true);
    }
  });
  emitLog('🔊 Bot listener started successfully.');
}

// === FORMAT MESSAGE (TAG SYSTEM) (IMPROVED ERROR HANDLING) ===
async function formatMessage(api, event, mainText) {
  const { senderID } = event;
  let senderName = `User-${senderID}`;

  try {
    const info = await api.getUserInfo(senderID);
    senderName = info?.[senderID]?.name || senderName;
  } catch (e) {
    emitLog('Error getting sender info: ' + e.message);
  }

  return {
    body: `@${senderName} ${mainText}\n\n— 💕𝑴𝑹 𝑷𝑹𝑰𝑵𝑪𝑬 💕\n------------------------------`,
    mentions: [{ tag: `@${senderName}`, id: senderID }]
  };
}

// === MESSAGE HANDLER (IMPROVED) ===
async function handleMessage(api, event) {
  const { threadID, senderID, body } = event;
  if (!body) return;
  const msg = body.toLowerCase().trim();
  
  try {
    const botID = api.getCurrentUserID && api.getCurrentUserID();
    if (senderID === botID) return;

    // === MULTI-TARGET SYSTEM: Check if user is in target list ===
    const targets = lockedTargets[threadID] || [];
    const isAdmin = senderID === adminID;
    const isCommand = body.startsWith(prefix);

    // Target Check Logic (UNCHANGED, as it fulfills the requirement)
    if (targets.length > 0) {
      if (targets.includes(senderID)) {
        // Target user: Proceed to normal conversation
      } else if (isAdmin && isCommand) {
        return await handleAdminCommand(api, event, body, isAdmin);
      } else {
        if (isCommand && !isAdmin) {
          const reply = await formatMessage(api, event, `You don't have permission to use commands while target is locked.`);
          await api.sendMessage(reply, threadID);
        }
        return; 
      }
    } else {
      // NO TARGET SET: Only admin commands are processed
      if (isCommand && isAdmin) {
        return await handleAdminCommand(api, event, body, isAdmin);
      }
      return; 
    }

    // Avoid multiple replies in quick succession (spam stop)
    const now = Date.now();
    if (lastMessageTime[threadID] && now - lastMessageTime[threadID] < 1500) return;
    lastMessageTime[threadID] = now;

    // === Normal conversation flow for target user ===
    if (!conversationState[threadID]) conversationState[threadID] = 0;
    let replyBody = null;

    if (conversationState[threadID] === 0 && msg.includes('hello')) {
      replyBody = 'hello I am fine';
      conversationState[threadID] = 1;
    } else if (conversationState[threadID] === 1 && (msg.includes('hi kaise ho') || msg.includes('kese ho'))) {
      replyBody = 'thik hu tum kaise ho';
      conversationState[threadID] = 0;
    } else {
      // === MASTI AUTO REPLY for target user ===
      const randomReply = mastiReplies[Math.floor(Math.random() * mastiReplies.length)];
      replyBody = randomReply;
    }

    const styled = await formatMessage(api, event, replyBody);
    await api.sendMessage(styled, threadID);

  } catch (e) {
    emitLog('⚠️ Error in handleMessage: ' + e.message, true);
  }
}

// === ADMIN COMMAND HANDLER (ROBUST) ===
async function handleAdminCommand(api, event, body, isAdmin) {
  const { threadID } = event;
  
  try {
    const args = body.slice(prefix.length).trim().split(/ +/);
    const command = args.shift().toLowerCase();

    // Command routing
    if (command === 'group') return handleGroupCommand(api, event, args, isAdmin);
    if (command === 'nickname') return handleNicknameCommand(api, event, args, isAdmin);
    if (command === 'target') return handleTargetCommand(api, event, args, isAdmin);
    if (command === 'autoadd') return handleAutoAddCommand(api, event, args, isAdmin);

    // Help Command
    const helpText = `‎═══════════════════
/group on/off → LOCK GROUP NAME
/nickname on/off → LOCK NICKNAME
/target add/rem/list/clear <userID> → MULTI TARGET
/autoadd on/off/list <userID> → AUTO ADD USER
═══════════════════
Prefix: ${prefix} | Admin: ${adminID}`;
    const help = await formatMessage(api, event, helpText);
    return api.sendMessage(help, threadID);
  } catch (e) {
    emitLog('⚠️ Error in handleAdminCommand: ' + e.message, true);
    return api.sendMessage(`Command failed: ${e.message}`, threadID);
  }
}

// **--- (Group, Nickname, Target, AutoAdd Command Handlers are kept the same as they were robust enough) ---**
// ... [Remaining command handlers (handleGroupCommand, handleNicknameCommand, handleTargetCommand, handleAutoAddCommand) go here] ...
// I will include the existing command functions here for completeness:

// === GROUP COMMAND ===
async function handleGroupCommand(api, event, args, isAdmin) {
  const { threadID } = event;
  if (!isAdmin) return api.sendMessage(await formatMessage(api, event, 'Permission denied: admin only.'), threadID);

  const sub = (args.shift() || '').toLowerCase();
  if (sub === 'on') {
    const name = args.join(' ').trim();
    if (!name) return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}group on <name>`), threadID);
    lockedGroups[threadID] = name;
    try { await api.setTitle(name, threadID); } catch(e) { emitLog('Group set title error: ' + e.message); }
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, `Group name locked to "${name}".`), threadID);
  } else if (sub === 'off') {
    delete lockedGroups[threadID];
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, 'Group name unlocked.'), threadID);
  } else {
    return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}group on/off`), threadID);
  }
}

// === NICKNAME COMMAND ===
async function handleNicknameCommand(api, event, args, isAdmin) {
  const { threadID } = event;
  if (!isAdmin) return api.sendMessage(await formatMessage(api, event, 'Permission denied: admin only.'), threadID);

  const sub = (args.shift() || '').toLowerCase();
  if (sub === 'on') {
    const nick = args.join(' ').trim();
    if (!nick) return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}nickname on <nick>`), threadID);
    lockedNicknames[threadID] = nick;
    try {
      const info = await api.getThreadInfo(threadID);
      for (const pid of info.participantIDs || []) {
        // Exclude Admin from nickname lock, as it might interfere with commands
        if (pid !== adminID) { 
          await api.changeNickname(nick, threadID, pid);
          await new Promise(r => setTimeout(r, 200));
        }
      }
    } catch(e) { emitLog('Nickname set error: ' + e.message); }
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, `Nicknames locked to "${nick}".`), threadID);
  } else if (sub === 'off') {
    delete lockedNicknames[threadID];
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, 'Nickname lock disabled.'), threadID);
  } else {
    return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}nickname on/off`), threadID);
  }
}

// === TARGET COMMAND (UPDATED FOR MULTI-TARGET) ===
async function handleTargetCommand(api, event, args, isAdmin) {
  const { threadID } = event;
  if (!isAdmin) return api.sendMessage(await formatMessage(api, event, 'Permission denied: admin only.'), threadID);

  const sub = (args.shift() || '').toLowerCase();
  if (!lockedTargets[threadID]) {
    lockedTargets[threadID] = [];
  }

  if (sub === 'add') {
    const userID = args.join(' ').trim();
    if (!userID) {
      return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}target add <userID>`), threadID);
    }
    if (!lockedTargets[threadID].includes(userID)) {
      lockedTargets[threadID].push(userID);
      saveConfig();
      return api.sendMessage(await formatMessage(api, event, `Added "${userID}" to target list. Current targets: ${lockedTargets[threadID].join(', ')}`), threadID);
    } else {
      return api.sendMessage(await formatMessage(api, event, `User "${userID}" is already in target list.`), threadID);
    }

  } else if (sub === 'rem' || sub === 'remove') {
    const userID = args.join(' ').trim();
    if (!userID) {
      return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}target rem <userID>`), threadID);
    }
    const index = lockedTargets[threadID].indexOf(userID);
    if (index > -1) {
      lockedTargets[threadID].splice(index, 1);
      saveConfig();
      return api.sendMessage(await formatMessage(api, event, `Removed "${userID}" from target list. Current targets: ${lockedTargets[threadID].join(', ') || 'None'}`), threadID);
    } else {
      return api.sendMessage(await formatMessage(api, event, `User "${userID}" not found in target list.`), threadID);
    }

  } else if (sub === 'list') {
    if (lockedTargets[threadID] && lockedTargets[threadID].length > 0) {
      return api.sendMessage(await formatMessage(api, event, `Current targets: ${lockedTargets[threadID].join(', ')}`), threadID);
    } else {
      return api.sendMessage(await formatMessage(api, event, 'No targets set. Bot will not reply to anyone.'), threadID);
    }

  } else if (sub === 'clear') {
    lockedTargets[threadID] = [];
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, 'Target list cleared. Bot will not reply to anyone.'), threadID);

  } else {
    return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}target add/rem/list/clear <userID>`), threadID);
  }
}

// === AUTO ADD COMMAND ===
async function handleAutoAddCommand(api, event, args, isAdmin) {
  const { threadID } = event;
  if (!isAdmin) return api.sendMessage(await formatMessage(api, event, 'Permission denied: admin only.'), threadID);

  const sub = (args.shift() || '').toLowerCase();
  
  if (sub === 'on') {
    const userID = args.join(' ').trim();
    if (!userID) {
      return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}autoadd on <userID>`), threadID);
    }
    if (!autoAddUsers[threadID]) {
      autoAddUsers[threadID] = {};
    }
    autoAddUsers[threadID][userID] = true;
    saveConfig();
    return api.sendMessage(await formatMessage(api, event, `Auto-add enabled for user ${userID}. Bot will automatically add this user back if they leave.`), threadID);
    
  } else if (sub === 'off') {
    const userID = args.join(' ').trim();
    if (!userID) {
      return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}autoadd off <userID>`), threadID);
    }
    if (autoAddUsers[threadID] && autoAddUsers[threadID][userID]) {
      delete autoAddUsers[threadID][userID];
      if (Object.keys(autoAddUsers[threadID]).length === 0) {
        delete autoAddUsers[threadID];
      }
      saveConfig();
      return api.sendMessage(await formatMessage(api, event, `Auto-add disabled for user ${userID}.`), threadID);
    } else {
      return api.sendMessage(await formatMessage(api, event, `Auto-add was not enabled for user ${userID}.`), threadID);
    }
    
  } else if (sub === 'list') {
    if (!autoAddUsers[threadID] || Object.keys(autoAddUsers[threadID]).length === 0) {
      return api.sendMessage(await formatMessage(api, event, 'No users in auto-add list for this group.'), threadID);
    }
    const userList = Object.keys(autoAddUsers[threadID]).join(', ');
    return api.sendMessage(await formatMessage(api, event, `Auto-add users in this group:\n${userList}`), threadID);
    
  } else {
    return api.sendMessage(await formatMessage(api, event, `Usage: ${prefix}autoadd on/off/list <userID>`), threadID);
  }
}

// === HANDLE USER LEFT GROUP (ROBUST) ===
async function handleUserLeftGroup(api, event) {
  const { threadID, logMessageData } = event;
  
  if (logMessageData?.leftParticipantFbId && autoAddUsers[threadID]) {
    const leftUserID = String(logMessageData.leftParticipantFbId);
    
    if (autoAddUsers[threadID][leftUserID]) {
      emitLog(`Auto-adding user ${leftUserID} back to group ${threadID}`);
      
      try {
        await api.addUserToGroup(leftUserID, threadID);
        // Wait a moment before sending the message to ensure user is added
        await new Promise(r => setTimeout(r, 1000)); 
        await api.sendMessage(await formatMessage(api, event, `Automatically added user ${leftUserID} back to the group.`), threadID);
        emitLog(`Successfully auto-added user ${leftUserID} to group ${threadID}`);
      } catch (error) {
        emitLog(`Failed to auto-add user ${leftUserID}: ${error.message}`, true);
        await api.sendMessage(await formatMessage(api, event, `Failed to auto-add user ${leftUserID}. They may have privacy restrictions or bot cannot add them.`), threadID);
      }
    }
  }
}

// === AUTO RESTORE (ROBUST) ===
async function setBotNicknamesInGroups() {
  if (!botAPI) return;
  try {
    const threads = await botAPI.getThreadList(100, null, ['GROUP']);
    const botID = botAPI.getCurrentUserID();
    for (const thread of threads) {
      // Use getThreadInfo for more reliable nickname check
      const info = await botAPI.getThreadInfo(thread.threadID).catch(e => {
        emitLog(`Could not get info for thread ${thread.threadID}: ${e.message}`);
        return null;
      });
      if (!info) continue;

      if (info?.nicknames?.[botID] !== botNickname) {
        await botAPI.changeNickname(botNickname, thread.threadID, botID).catch(e => {
          emitLog(`Failed to set bot nickname in ${thread.threadID}: ${e.message}`);
        });
        emitLog(`Bot nickname set in ${thread.threadID}`);
      }
      await new Promise(r => setTimeout(r, 200));
    }
  } catch (e) {
    emitLog('⚠️ Nickname set/restore error: ' + e.message, true);
  }
}

// === THREAD NAME LOCK (ROBUST) ===
async function handleThreadNameChange(api, event) {
  const { threadID, authorID } = event;
  try {
    const newTitle = event.logMessageData?.name;
    const lockedTitle = lockedGroups[threadID];
    if (lockedTitle && authorID !== adminID && newTitle !== lockedTitle) {
      await api.setTitle(lockedTitle, threadID);
      // Send a mention to the user who tried to change it
      const user = await api.getUserInfo(authorID).catch(() => ({}));
      const name = user?.[authorID]?.name || 'User';
      await api.sendMessage({ 
          body: `@${name} group name locked!`, 
          mentions: [{ tag: name, id: authorID }] 
      }, threadID);
    }
  } catch (e) {
    emitLog('⚠️ Error in handleThreadNameChange: ' + e.message, true);
  }
}

// === NICKNAME LOCK (ROBUST) ===
async function handleNicknameChange(api, event) {
  const { threadID, authorID, participantID, newNickname } = event;
  const botID = api.getCurrentUserID();
  
  try {
    // 1. Bot's Nickname Lock
    if (participantID === botID && authorID !== adminID && newNickname !== botNickname) {
      await api.changeNickname(botNickname, threadID, botID);
      return;
    }
    
    // 2. Participant Nickname Lock
    const lockedNick = lockedNicknames[threadID];
    if (lockedNick && authorID !== adminID && newNickname !== lockedNick) {
      // Only lock if the change wasn't made by the Admin
      await api.changeNickname(lockedNick, threadID, participantID);
    }
  } catch (e) {
    emitLog('⚠️ Error in handleNicknameChange: ' + e.message, true);
  }
}

// === BOT ADDED (ROBUST) ===
async function handleBotAddedToGroup(api, event) {
  const { threadID, logMessageData } = event;
  try {
    const botID = api.getCurrentUserID();
    if (logMessageData?.addedParticipants?.some(p => String(p.userFbId) === String(botID))) {
      await api.changeNickname(botNickname, threadID, botID);
      await api.sendMessage(`Hello! I'm online. Use ${prefix}group, ${prefix}nickname, ${prefix}target or ${prefix}autoadd to manage locks.`, threadID);
    }
  } catch (e) {
    emitLog('⚠️ Error in handleBotAddedToGroup: ' + e.message, true);
  }
}

// === DASHBOARD & CONFIGURATION ===
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(express.static('public'));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.post('/configure', (req, res) => {
  try {
    const cookies = typeof req.body.cookies === 'string' ? JSON.parse(req.body.cookies) : req.body.cookies;
    prefix = req.body.prefix || prefix;
    adminID = req.body.adminID || adminID;
    
    // Basic validation
    if (!Array.isArray(cookies) || cookies.length === 0) {
        return res.status(400).send('Invalid cookies: Expected a JSON array.');
    }
    if (!adminID) return res.status(400).send('adminID required');
    
    currentCookies = cookies;
    saveConfig();
    res.send('Configured. Starting bot...');
    initializeBot(currentCookies, prefix, adminID);
  } catch (e) {
    emitLog('🚨 Config POST error: ' + e.message, true);
    res.status(400).send('Invalid data format or internal error.');
  }
});

// === AUTO LOAD CONFIG ===
function loadConfigAndStart() {
    try {
        if (fs.existsSync('config.json')) {
            const loaded = JSON.parse(fs.readFileSync('config.json', 'utf8'));
            if (loaded.botNickname) botNickname = loaded.botNickname;
            if (loaded.prefix) prefix = loaded.prefix;
            if (loaded.adminID) adminID = loaded.adminID;
            if (loaded.lockedGroups) lockedGroups = loaded.lockedGroups;
            if (loaded.lockedNicknames) lockedNicknames = loaded.lockedNicknames;
            if (loaded.lockedTargets) lockedTargets = loaded.lockedTargets;
            if (loaded.autoAddUsers) autoAddUsers = loaded.autoAddUsers;
            
            if (Array.isArray(loaded.cookies) && loaded.cookies.length && loaded.adminID) {
                currentCookies = loaded.cookies;
                emitLog('Found saved configuration; starting bot.');
                initializeBot(currentCookies, prefix, adminID);
            } else {
                emitLog('No valid cookies/adminID found. Configure via dashboard.');
            }
        } else {
            emitLog('No config.json found. Configure via dashboard.');
        }
    } catch (e) {
        emitLog('🚨 Config load error: ' + e.message, true);
    }
}

// === SERVER STARTUP ===
loadConfigAndStart();

const PORT = process.env.PORT || 20018;
server.listen(PORT, () => emitLog(`🌐 Server running on port ${PORT}`));
io.on('connection', socket => {
  emitLog('💻 Dashboard connected');
  socket.emit('botlog', `Bot status: ${botAPI ? 'Started' : 'Not started'} | AdminID: ${adminID}`);
});


// === GLOBAL UNHANDLED ERROR CATCHERS (CRASH PROTECTION) ===

// Catch unhandled promises (async errors)
process.on('unhandledRejection', (reason, promise) => {
    emitLog(`🛑 Unhandled Rejection at: ${promise}, reason: ${reason}`, true);
    // PM2 will handle the restart, but log the error
    // DO NOT EXIT here, let PM2 handle the process health check.
});

// Catch synchronous exceptions
process.on('uncaughtException', err => {
    emitLog(`🛑 Uncaught Exception: ${err.message}\n${err.stack}`, true);
    // This is a critical error. PM2 should restart the process.
    // Clean up before crashing (optional but good practice)
    if (botAPI && botAPI.stopListening) {
        try { botAPI.stopListening(); } catch {}
    }
    // Let the process manager (PM2) terminate and restart
    // If running without PM2, you might use: process.exit(1);
});
