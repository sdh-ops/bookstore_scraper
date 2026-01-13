# yes24scraper

This repository runs a YES24 scraper on GitHub Actions with a web OTP input.

Overview
- Manual trigger via Actions (`workflow_dispatch`).
- During run, the Action starts a small Flask app (OTP web UI) and exposes it via `cloudflared`.
- Staff open the public URL, select a phone number, enter the SMS OTP, and submit.
- The in-process scraper reads the selected phone and OTP and continues the scraping.

Setup
1. Add repository Secrets:
   - `YES24_ID` and `YES24_PASSWORD` (scraper credentials)
   - `GOOGLE_CREDENTIALS` (service account JSON content)

2. Start workflow: GitHub → Actions → YES24 Scraper → Run workflow.

Notes
- The runner will print the public OTP URL into the Action logs. Share that URL with the staff who will input the OTP.
- For security, avoid sharing the URL publicly. Consider integrating webhook or Slack notifications to distribute the URL privately.
