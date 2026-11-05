# ERA5 Pakistan Atmospheric Reanalysis Bulk Scheduler

Automated, non-blocking, rate-limit-aware scheduler that requests and downloads the complete **ERA5 atmospheric reanalysis** record over **Pakistan** (`[37.5°N, 60.5°E, 23.5°N, 78.0°E]`, `0.25° × 0.25°` grid, hourly `00:00–23:00 UTC`, `1940-01` to present):

1. **All 37 Standard Pressure Levels (`reanalysis-era5-pressure-levels`)**
   - **Levels (`37`)**: `1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000` hPa.
   - **Variables (`16`)**: `geopotential`, `temperature`, `u_component_of_wind`, `v_component_of_wind`, `vertical_velocity`, `vorticity`, `divergence`, `potential_vorticity`, `relative_humidity`, `specific_humidity`, `fraction_of_cloud_cover`, `ozone_mass_mixing_ratio`, `specific_cloud_liquid_water_content`, `specific_cloud_ice_water_content`, `specific_rain_water_content`, `specific_snow_water_content`.
   - **Chunking (`55,056 / 60,000` CDS field limit)**: 2 variables × 1 month × 24 hours × 37 levels (`8` chunks/month).

2. **All 137 MARS Model Levels (`reanalysis-era5-complete`)**
   - **Levels (`137`)**: `1/to/137` (`levtype=ml`, `class=ea`, `stream=oper`, `type=an`).
   - **Parameters (`16`)**: `75` (`crwc`), `76` (`cswc`), `77` (`etadot`), `129` (`z`), `130` (`t`), `131` (`u`), `132` (`v`), `133` (`q`), `135` (`w`), `138` (`vo`), `152` (`lnsp`), `155` (`d`), `203` (`o3`), `246` (`clwc`), `247` (`ciwc`), `248` (`cc`).
   - **Chunking (`105,216 / 120,000` CDS MARS cost limit)**: all 16 parameters × 137 levels × 24 hours × 1 day (`1` chunk/day).

---

## Key Capabilities

- **Programmatic Terms of Service / Licence Acceptance**: Calls `client.accept_licence(licence_id, revision)` automatically via the CDS API (`cc-by` rev 1, `terms-of-use-cds` rev 13, `licence-to-use-copernicus-products` rev 12, `data-protection-privacy-statement` rev 24 are all verified and accepted).
- **1.5-Hour Time-Separated Polling (Never Waits on CDS)**: Requests are submitted asynchronously via `client.submit()`. A submitted request is **never** polled until at least **`90 minutes` (1.5 hours)** (`min_age_before_poll_minutes: 90`) have passed since submission, and thereafter at most once every `25 minutes`. Each scheduler tick finishes in seconds when only polling/submitting.
- **Zero Overlap with Prior Accepted/Successful CDS Requests**: Before submitting any chunk, `scan_jobs()` inspects existing jobs on your CDS account (`accepted`, `running`, `successful`):
  - Identical requests are adopted rather than re-submitted.
  - Overlapping days/variables (such as your prior `2026-01-01` request `29079e6e-8b29-4045-8d86-346a3d8d1623`) are automatically subtracted per variable so already-provided data is never requested again.
- **Strict Cost-Limit & Queue Protection**:
  - Calls `client.estimate_costs(collection, request)` before every submission and automatically splits any chunk that exceeds `97%` of the CDS limit.
  - Caps concurrent in-flight jobs at `4` for pressure levels and `2` for MARS model levels (`max_submissions_per_run: 6`).
- **Google Drive Direct Streaming OR Manual Download Mode**:
  - When Google Drive credentials (`GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`) are configured, ready files are streamed directly from the CDS object store to `ERA5_Pakistan/` on Google Drive using 64 MB resumable chunks (zero local disk footprint) and verified against the CDS `file:checksum` MD5 hash.
  - When Google Drive credentials are not yet set (`storage: "auto"` -> `"none"`), the scheduler still submits and monitors CDS requests and writes all ready download links to [`state/ready_for_manual_download.csv`](state/ready_for_manual_download.csv) (you can also pull them anytime from <https://cds.climate.copernicus.eu/requests>).

---

## Quick Commands (Windows PowerShell)

```powershell
# Run one scheduler tick (licences -> scan prior jobs -> poll >=90m jobs -> transfer -> submit new)
.\run_tick.ps1 run

# Preview the next 10 chunks in the queue without submitting
.\run_tick.ps1 plan -n 10

# Print current progress table
.\run_tick.ps1 status

# (Manual download mode) Mark ready chunks as pulled after you download them from CDS
.\run_tick.ps1 mark-pulled --all

# Install / update the Windows Scheduled Task (runs every 30 minutes in background)
.\install_windows_task.ps1
```

---

## Optional: Connect Google Drive

To have the scheduler stream completed GRIB files directly into your Google Drive (`ERA5_Pakistan/pressure_levels/<YYYY>/` and `ERA5_Pakistan/model_levels/<YYYY>/`):

1. Create a free Desktop OAuth Client ID in [Google Cloud Console](https://console.cloud.google.com/apis/credentials) with the **Google Drive API** enabled.
2. Run the one-time setup script:
   ```powershell
   .\.venv\bin\python.exe drive_auth_setup.py
   ```
3. It will save `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, and `GDRIVE_REFRESH_TOKEN` to `secrets.local.json`. On the very next tick, `storage: "auto"` will automatically switch from `"none"` to `"drive"`.

---

## Optional: Run via GitHub Actions

A complete workflow is included at [`.github/workflows/era5-scheduler.yml`](.github/workflows/era5-scheduler.yml):

1. Push this directory to a GitHub repository (`secrets.local.json` is excluded via `.gitignore`).
2. Under **Settings -> Secrets and variables -> Actions**, add:
   - `CDSAPI_KEY`: `cc6584f6-88b0-4d75-b89f-8fe56ad362fe`
   - *(Optional for Google Drive streaming)* `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`
3. Under **Settings -> Actions -> General -> Workflow permissions**, select **Read and write permissions** so the workflow can commit updated `state/` shards after each 30-minute tick.
