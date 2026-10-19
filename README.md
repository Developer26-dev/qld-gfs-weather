# Queensland public GFS weather publisher

A small, public NOAA/NCEP GFS 0.25° wind/gust processing project. It builds and validates a static Queensland forecast dataset. No private application code, accounts, user profiles, photographs, locations or API secrets belong in this repository.

**Activation requires owner approval.** The workflow is disabled by default until repository variable `ENABLE_WEATHER_PIPELINE` is explicitly set to `true`. This package is prepared for cloud validation; a local test is not evidence of hosted scheduling reliability.

## Architecture

Standard GitHub-hosted Ubuntu runner → bounded NOAA subset/validation → one complete public-weather Git commit → Cloudflare Pages native Git integration → static HTTPS JSON.

No Cloudflare API token is used by this workflow. The native Cloudflare GitHub application should be granted access only to this repository. GitHub's ephemeral `GITHUB_TOKEN` needs Contents: write for this repository only. No Functions, Worker, KV, R2, D1, custom domains, paid runners, Actions artifacts or Actions caches are used. Public standard runner execution is free under the current GitHub policy; Pages static asset requests do not invoke Worker CPU.

## Schedule and limits

Four opportunities/day: `40 4,10,16,22 * * *` UTC (14:40, 20:40, 02:40, 08:40 Brisbane). These are weather-feed refresh times, unrelated to any application notification schedule. Actual NOAA model availability is checked. GitHub schedules can be delayed or dropped and may be disabled after 60 days without repository activity; no exact-time SLA is claimed. Genuine weather/status commits create repository activity, but ongoing schedule health still needs checking.

The workflow serializes jobs and first commits its scheduled-opportunity reservation with `[CF-Pages-Skip]`. This persists the attempt before external downloads and skips the unnecessary Pages build. Same-cycle repeats do no NOAA downloads. A crash after reservation waits for the next six-hour opportunity. Manual setup runs reserve the preceding scheduled opportunity, so they cannot consume the next scheduled refresh. Retries are bounded to the next opportunity rather than repeatedly hammering NOAA. One subsequent commit publishes the complete forecast/status; normal volume is up to 124 Pages builds per 31-day month, plus initial setup. Disable preview-branch builds.

NOAA: bbox 152.5–154°E / 28–26°S; 63 native nodes. Fetch only 10 m U/V and surface gust. Seven days, native three-hour output plus 06:00 Brisbane when native hourly leads exist. No fabricated hourly fields. At most 62 frames, 70 requests, 2 MB per attempt and 750 seconds acquisition time. Wait ten seconds after each completed HTTP fetch. Any timeout, incomplete set or mismatched GRIB metadata fails safely.

## Publication contract

- `manifest.json`: schema, provider/model/run/valid times, expiry, validator version, SHA-256, size and versioned dataset path.
- `runs/gfs-<run>-<digest>.json`: immutable compact validated dataset; current plus one previous version.
- `current.json`: convenience alias containing the same bytes as the manifest's current version.
- `status.json`: latest attempt/result, next expected opportunity, overdue deadline, source and job identity.
- `index.html`: status page calculates stale/missed updates against the viewer's clock.

One Git commit and one Pages deployment contain all files. `python3 validate.py public` is the Pages build gate; it uses only Python's standard library. Set Pages build variable `SKIP_DEPENDENCY_INSTALL=1` to avoid installing the runner-only decoder dependencies on Pages. The validator checks hashes, aliases, grid metadata, units, native timestamps, limits and a static-file allowlist. It rejects `_worker.js`, functions and unrelated files. A failed build must leave the prior Pages deployment active. The workflow then reads the remote manifest/status/grid, validates SHA-256, CORS, revalidation/immutable cache headers and an ETag 304 response, with a bounded ten-minute wait. Actual hosted behaviour must be verified after provisioning.

Data expires 18 hours after the model run; an old complete file may stay published as historical evidence but the consuming Map must hide its animation after expiry. Status cannot substitute for the forecast's own timestamp. Clients should use public requests without credentials and let browser HTTP caching handle ETags; do not add custom cross-origin conditional headers that need an OPTIONS preflight. No private PWA cookie is required by this feed.

## Approved setup steps (do not run before approval)

1. Create a separate **public** repository from only this package; default branch `main`. Never publish a private app checkout or its Git history.
2. Create a **Pages Free Git-integrated** project linked only to this repository. Production branch `main`, build command `python3 validate.py public`, output `public`; preview branch builds disabled. No Functions or bindings.
3. Set repository variable `WEATHER_BASE_URL` to the actual allocated `https://…pages.dev` address and set `ENABLE_WEATHER_PIPELINE=true`. Names/URLs are not evidence of provisioning.
4. Run one workflow manually, then observe multiple scheduled model cycles from GitHub's runner. Confirm public visibility and standard Ubuntu runner, account/project Free eligibility, actual build counts, CORS/ETag/HTTPS, failed-build recovery and zero Functions invocations. Leave all billing settings unchanged.
5. Only after remote evidence passes, integrate the consumer and request separate application production deployment approval.

The current account connection might need an owner-led GitHub login and Cloudflare GitHub-app installation. Do not paste credentials into issues, workflow files, chat or repository variables. No long-lived API token is part of this design.

## Local validation

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python validate.py public
```

`pipeline.py bootstrap` creates an explicitly unavailable initial site. `pipeline.py reserve` then `pipeline.py run` performs real acquisition and writes only this working directory. The workflow commits the reservation before calling `run` so hosted job restarts cannot bypass the limit. The standalone decoder CLI remains guarded for research output directories. Tests use temporary directories and labelled synthetic time variants of a real NOAA sample; they never publish QA weather.

## Failure visibility and operating limits

Failed acquisition is published as a failed refresh status while retaining the last complete dataset; the workflow then exits unsuccessfully. Publication failure is an unsuccessful workflow and leaves the prior site. A later scheduled cycle retries normal publication. GitHub workflow notifications depend on the owner's existing notification settings; this project does not change them or send messages separately. A dropped/disabled schedule cannot notify through a job that never runs: the public status page and consuming app detect missed deadlines and expiry. A separate external uptime monitor is not provisioned.

Inspect Actions run history, runner durations and job summaries; Pages deployment history and the published job/next-expected timestamps; monthly build consumption; static response bytes; and repository growth. Up to four 89 KB weather commits/day would add roughly 11 MB/month before Git compression (plus bounded status history). Working data is bounded, Git history is not automatically deleted. Reassess size well before repository hosting limits. This deliberately avoids paid Actions artifact/cache storage.

NOAA data attribution: https://nomads.ncep.noaa.gov/ and https://registry.opendata.aws/noaa-gfs-bdp-pds/. Compact JSON is a converted/subset representation, not an unaltered NOAA file or NOAA endorsement. Data must not be interpreted as local crossing, shelter or navigation advice.
