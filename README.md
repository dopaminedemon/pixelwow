# Whisperwood – shared multiplayer world

Four files make the whole thing:

| File | What it is |
|---|---|
| `game.html` | The game (what players load in their browser). Has the login screen, party panel, chat. |
| `server.js` | The server. Runs the world, monsters, claiming, parties, chat, accounts. No libraries needed. |
| `world.json` | World data the server reads (zones, monster stats, spawn plan). |
| `package.json` | Lets hosting services start it (`npm start`). |

The server also creates a `data/` folder next to itself. **That folder is your save game**: `accounts.json` holds every account and character (passwords are stored hashed), and `backup-…json` files are hourly copies.

## 1. Run it on your own computer (free)

1. Install **Node.js** (the LTS version, 18 or newer) from https://nodejs.org
2. Put all the files in one folder.
3. Start the server:
   - Windows: double-click `start.bat`
   - Mac/Linux: run `sh start.sh` (or `node server.js`) in that folder
4. Open **http://localhost:3000** in your browser. Click **Register**, make an account, create a character, play.

To test multiplayer alone: open a second browser window (or a private window), register a second account, and play both. Try:
- Hit the same monster from both windows – the second player is told the monster is claimed.
- Click the other player's character → *Invite to party*. Accept in the other window, then kill something together (both get XP, the XP is split).
- Press **Enter** to chat. `/help` lists commands.

## 2. Let friends join

**Same Wi-Fi / home network:** they open `http://YOUR-COMPUTER-IP:3000` (e.g. `http://192.168.1.20:3000`). Allow Node.js through your firewall if asked.

**Over the internet, no router setup:** use a free tunnel that gives your computer a public https link, then send that link to your friends:
- Cloudflare Tunnel: `cloudflared tunnel --url http://localhost:3000`
- or ngrok: `ngrok http 3000`

The world is online only while your computer and the `node server.js` window are running. Saves live on your computer, so they survive restarts. (Free tunnel terms and limits change – check the service's current rules.)

## 3. Run it 24/7 on a host

Any host that runs Node.js and keeps a **persistent disk** works (a small VPS, Railway, Fly.io, an Oracle/Google free VM). Upload the files, set the start command to `node server.js`, and point `DATA_DIR` at the persistent disk. Hosts that wipe their disk on restart (e.g. Render's free tier) will lose characters unless you add storage.

## Settings (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | 3000 | Port to listen on |
| `MAX_PLAYERS` | 40 | Players allowed in the world at once |
| `INVITE_CODE` | (none) | If set, new accounts must type this code to register |
| `DATA_DIR` | `./data` | Where accounts/characters are saved |

Example: `INVITE_CODE=letmein MAX_PLAYERS=20 node server.js`
(Windows PowerShell: `$env:INVITE_CODE="letmein"; node server.js`)

`http://localhost:3000/health` shows how many players are online.

## What is shared, and how it works

**Decided by the server (same for everyone):**
- All ~600 monsters: positions, health, AI, aggro, respawns, bosses.
- **Claiming:** the first player to hit a monster owns it (and their party). Others can't damage it until it dies or resets.
- Kill credit: the owner and party members nearby share the XP and gold and get quest kill-credit.
- Herb/ore nodes (when someone picks one, it's gone for everyone until it respawns).
- The day/night clock, other players' positions, chat, parties, accounts, saved characters.

**Still handled on each player's own computer:**
- Their stats, inventory, gold, quests, skills (saved to the server every 5 seconds).
- Damage numbers (the server caps each hit by level and limits damage per second, but it can't verify them exactly).
- Loot is rolled personally for each killer, so nobody steals loot.
- Necromancer summons, turrets and totems only exist on their owner's screen; monsters cannot target them.
- Quest ambushes (e.g. the shrines) are private to the player doing the quest.

**Be honest about security:** this version stops the easy cheats (infinite damage, stealing monsters, editing another player's character) but a determined player could still edit their own gold or items in their browser. Fine for friends; not for a public game with prizes.

## Troubleshooting

- *"Cannot reach the server"* – the server isn't running, or you opened `game.html` as a file. Always open the address the server prints (http://localhost:3000). The login screen has a "Play offline" button for single-player.
- *Port already in use* – run with another port: `PORT=3001 node server.js`.
- *Everyone sees a blank/black game* – the page loads three.js from the internet (cdnjs); players need internet access.
- *Lost connection* – the game reconnects automatically; if the server restarted you'll be asked to log in again.
- To wipe everything: stop the server and delete the `data` folder.
