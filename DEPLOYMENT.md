# Deploying on Ubuntu LTS with Cloudflare Tunnel

Complete beginner guide to hosting the University Dashboard (and future apps)
on an Ubuntu laptop using your domain `visiometrica.com`.

## What we're building

```
Browser → visiometrica.com (Cloudflare) → Cloudflare Tunnel → Caddy → uvicorn
```

- **Cloudflare** handles DNS + HTTPS + security
- **Cloudflare Tunnel** connects your laptop to Cloudflare (no port forwarding)
- **Caddy** is the reverse proxy (routes subdomains to the right app)
- **uvicorn** runs the FastAPI app

---

## Step 1: Prepare the Ubuntu laptop

Open a terminal on the Ubuntu laptop. Run these commands one at a time.

### 1.1 Update the system

```bash
sudo apt update && sudo apt upgrade -y
```

### 1.2 Install Python and required tools

```bash
sudo apt install -y python3 python3-venv python3-pip git curl
```

### 1.3 Check Python is installed

```bash
python3 --version
```

You should see something like `Python 3.10.x` or `3.12.x`. Any 3.10+ is fine.

---

## Step 2: Get the project on the laptop

### 2.1 Choose where to put it

```bash
mkdir -p ~/apps
cd ~/apps
```

### 2.2 Copy the project

You have two options:

**Option A — If you push it to GitHub first (recommended):**

```bash
git clone https://github.com/YOUR_USERNAME/university-dashboard.git
cd university-dashboard
```

**Option B — Copy from your Mac via USB or scp:**

From your Mac terminal:
```bash
scp -r /Users/victor/Documents/Projects/university_dashboard ubuntu-user@LAPTOP_IP:~/apps/
```

Then on the Ubuntu laptop:
```bash
cd ~/apps/university_dashboard
```

### 2.3 Set up the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2.4 Test that it runs

```bash
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```

Open a browser on the laptop and go to `http://localhost:8000`. You should see
the dashboard. Press `Ctrl+C` to stop it.

---

## Step 3: Create a systemd service

This makes the app start automatically when the laptop boots, and restart if it
crashes.

### 3.1 Find your username

```bash
whoami
```

Remember this — we'll call it `YOUR_USER` below.

### 3.2 Create the service file

```bash
sudo nano /etc/systemd/system/uni-dashboard.service
```

Paste this (replace `YOUR_USER` with your actual username from step 3.1):

```ini
[Unit]
Description=University Dashboard
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/home/YOUR_USER/apps/university_dashboard
Environment="PATH=/home/YOUR_USER/apps/university_dashboard/.venv/bin:/usr/bin"
Environment="APP_ENV=production"
Environment="DEBUG=false"
ExecStart=/home/YOUR_USER/apps/university_dashboard/.venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Save the file: press `Ctrl+O`, then `Enter`, then `Ctrl+X` to exit nano.

### 3.3 Enable and start the service

```bash
sudo systemctl daemon-reload
sudo systemctl enable uni-dashboard
sudo systemctl start uni-dashboard
```

### 3.4 Check it's running

```bash
sudo systemctl status uni-dashboard
```

You should see `active (running)` in green. If you see an error, run:

```bash
sudo journalctl -u uni-dashboard -n 50
```

to see the logs and troubleshoot.

---

## Step 4: Install Caddy (reverse proxy)

Caddy will sit in front of your app and route subdomain traffic to the right
port.

### 4.1 Install Caddy

```bash
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install -y caddy
```

### 4.2 Configure Caddy

```bash
sudo nano /etc/caddy/Caddyfile
```

Delete everything in the file and paste this:

```
uni-dashboard.visiometrica.com {
    reverse_proxy 127.0.0.1:8000
}

# When you add more apps later, just add more blocks:
# analytics.visiometrica.com {
#     reverse_proxy 127.0.0.1:8001
# }
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`).

### 4.3 Restart Caddy

```bash
sudo systemctl restart caddy
sudo systemctl status caddy
```

Should show `active (running)`.

---

## Step 5: Set up Cloudflare

### 5.1 Create a Cloudflare account

1. Go to https://dash.cloudflare.com/sign-up
2. Create a free account

### 5.2 Add your domain to Cloudflare

1. In the Cloudflare dashboard, click **"Add a site"**
2. Enter `visiometrica.com`
3. Select the **Free** plan
4. Cloudflare will scan your existing DNS records — review them and click
   **Continue**
5. Cloudflare will give you **two nameservers**, something like:
   - `ada.ns.cloudflare.com`
   - `bob.ns.cloudflare.com`

### 5.3 Change nameservers at your registrar

Go to wherever you bought `visiometrica.com` (GoDaddy, Namecheap, etc.):

1. Find the **DNS** or **Nameservers** settings
2. Change the nameservers to the two Cloudflare gave you
3. Save

This can take up to 24 hours to propagate, but usually takes 15-60 minutes.
Cloudflare will email you when it's active.

### 5.4 Wait for activation

Go back to Cloudflare dashboard. Your domain status will change from "Pending"
to "Active". Do not proceed until this happens.

---

## Step 6: Install Cloudflare Tunnel

Back on the Ubuntu laptop terminal:

### 6.1 Install cloudflared

```bash
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o /tmp/cloudflared.deb
sudo dpkg -i /tmp/cloudflared.deb
```

### 6.2 Login to Cloudflare

```bash
cloudflared tunnel login
```

This opens a browser window. Select `visiometrica.com` and authorize it.
A certificate file is saved to `~/.cloudflared/cert.pem`.

### 6.3 Create a tunnel

```bash
cloudflared tunnel create laptop-server
```

This prints a tunnel ID (a long string like `a1b2c3d4-...`). **Write it down**.

### 6.4 Configure the tunnel

```bash
nano ~/.cloudflared/config.yml
```

Paste this (replace `TUNNEL_ID` with the ID from step 6.3):

```yaml
tunnel: TUNNEL_ID
credentials-file: /home/YOUR_USER/.cloudflared/TUNNEL_ID.json

ingress:
  - hostname: uni-dashboard.visiometrica.com
    service: http://127.0.0.1:8000

  # Add more apps here later:
  # - hostname: analytics.visiometrica.com
  #   service: http://127.0.0.1:8001

  # Catch-all (required, must be last):
  - service: http_status:404
```

Save and exit.

### 6.5 Create the DNS record

```bash
cloudflared tunnel route dns laptop-server uni-dashboard.visiometrica.com
```

This automatically creates a CNAME record in Cloudflare DNS pointing
`uni-dashboard.visiometrica.com` to your tunnel.

### 6.6 Install the tunnel as a system service

```bash
sudo cloudflared service install
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
```

### 6.7 Verify the tunnel is running

```bash
sudo systemctl status cloudflared
```

Should show `active (running)`.

---

## Step 7: Test it

Open a browser **on any device** (your phone, another computer) and go to:

```
https://uni-dashboard.visiometrica.com
```

You should see your dashboard with a valid HTTPS certificate.

---

## Adding a second app later

When you have another dashboard ready:

1. Run it on a new port (e.g., 8001)
2. Create a systemd service for it (copy step 3, change port and paths)
3. Add to `~/.cloudflared/config.yml`:
   ```yaml
   - hostname: new-app.visiometrica.com
     service: http://127.0.0.1:8001
   ```
4. Route the DNS:
   ```bash
   cloudflared tunnel route dns laptop-server new-app.visiometrica.com
   ```
5. Restart the tunnel:
   ```bash
   sudo systemctl restart cloudflared
   ```

That's it — the new app is live at `https://new-app.visiometrica.com`.

---

## Useful commands reference

```bash
# Check app status
sudo systemctl status uni-dashboard

# View app logs
sudo journalctl -u uni-dashboard -f

# Restart app after code changes
sudo systemctl restart uni-dashboard

# Check tunnel status
sudo systemctl status cloudflared

# View tunnel logs
sudo journalctl -u cloudflared -f

# Restart tunnel after config changes
sudo systemctl restart cloudflared
```

---

## Keeping the laptop running as a server

- **Disable sleep/suspend**: Settings → Power → set "Automatic Suspend" to Off
- **Disable screen lock**: optional, but saves resources
- **Close lid without sleeping**: run:
  ```bash
  sudo nano /etc/systemd/logind.conf
  ```
  Find and change (or add):
  ```
  HandleLidSwitch=ignore
  HandleLidSwitchExternalPower=ignore
  ```
  Then: `sudo systemctl restart systemd-logind`
- **Set a static local IP** (optional): makes it easier to SSH into from your
  other machines. Go to Settings → Network → Wired/WiFi → gear icon → IPv4 →
  set Manual and pick an IP like `192.168.1.100`

---

## Troubleshooting

**App won't start:**
```bash
cd ~/apps/university_dashboard
source .venv/bin/activate
uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```
Run it manually to see the error.

**Site shows 502 Bad Gateway:**
The tunnel is working but can't reach the app. Check that the app is running:
```bash
sudo systemctl status uni-dashboard
```

**Site shows "DNS not found":**
The DNS hasn't propagated yet. Wait a few minutes, or check that step 6.5
completed successfully:
```bash
cloudflared tunnel route dns laptop-server uni-dashboard.visiometrica.com
```

**Tunnel shows "connection refused":**
Make sure the port in `config.yml` matches the port in your systemd service.
