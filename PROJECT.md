# Vitals

This is an app to easily track my vitals like bloodwork and wellbeing

Pick the next most important feature to work on, implement it fully and test it if possible, tick it and commit when done. the Notes section is for you to comment on things that are worth remembering, more generic points add them to AGENTS.md. Update this document as requirements change or get clarified if needed

# Features:

- [x] blodwork importer: converts unstructured varied bloodwork pdfs into standardized json and uploads to s3
    - [x] use createScript.ts framework
    - [x] `scripts/bloodwork-import.ts` accepts a pdf as input, uses AI (gemini 3 flash via ai sdk + openrouter) to convert it to a standardized json template that includes date, location, lab name, import location (optional, flag), weight and the table of all standardized measurements. for each measurement capture value, ranges, flags, notes etc. store all the parsed data as `data/bloodwork_{date}_{lab}.json` pretty json 4 spaces
    - [x] account for the bloodwork being in different languages and vastly different formats output must be standardized and in engligh
    - [ ] when processing the data, also standardize the units, I want all the values to be the same for the same measurements. if a unit needs conversion, store an "original" value on it for reference.
    - [x] all data must also be uploaded to s3 in `stefan-life/vitals/bloodwork_{date}_{lab}.json` bucket location
    - [x] all my existing labs are in `data/to-import`, use them for testing and to get a sense of various potential formats. create a standard `BloodworkLab` zod type and use that as the basis for the various tools and enforce the json be in that shape. a lot of the properties will have to be optional most likely
    - [x] once everything is working and tested, import all data from `data/to-import`
- [x] data sync: downloads data from s3
    - [x] create a script to download data from the bucket via `scripts/download-data.ts`
    - [x] script only downloads what has changed
    - [x] when the server starts it would automatically call this script
- [x] historical dashboard: client application to analyze the data
    - [x] optimized for web, but also usable on mobile
    - [x] use ant-design components
    - [x] show a table of every single datapoint (names as rows, source as a column, from latest to oldest). allow filter
    - [x] allow starring measurements; starred rows are hoisted to the top and persisted in local storage
    - [x] show per-cell reference range visualization (min/max markers with current value marker) when range data exists
    - [x] allow selecting rows and columns (default to all columns) to see data on a chart, I want to see how the various vitals have trended over time. chart should show to the right of the table always visible if there are items selected
    - [x] in mobile portrait mode, only show the latest value and allow to switch the column to go to a different lab. checking items should show a chart under the table (chart always visible if values are selected so table takes up less space)
    - [ ] on desktop UI should take over the entire space and show all nutrients at once
- [x] use the aws cli to create a purpose built user for this project with dedicated permissions and store the credentials in .env.local
- [x] create env with env-manager and create specific keys for everything requested (you can use env-manager to generate an openrouter key)
- [x] bloodwork glossary enforcement: track canonical measurement names, aliases, and ranges with LLM-validated updates
    - [x] add `server/src/bloodwork-glossary.json` for known measurement vocabulary and range history
    - [x] during import, validate extracted measurement names against glossary and run a second LLM pass for unknown names
    - [x] classify unknown names as alias, valid new entry, or invalid parse and update glossary automatically
    - [x] enforce english-only canonical names/aliases in validator prompts and acceptance rules

# Notes:
- Importer defaults to `google/gemini-3-flash-preview` and validates output through the shared `BloodworkLab` schema.
- Importer runtime requires `OPENROUTER_API_KEY` and AWS env vars (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) unless running with `--skip-upload`.
- Dedicated IAM user `vitals-project-user` has been created with scoped access to `s3://stefan-life/vitals/*` and credentials are stored in `.env.local`.
- `scripts/download-data.ts` uses a local sync state file (`data/.s3-sync-state.json`) to only download changed S3 objects.
- Importer now maintains `server/src/bloodwork-glossary.json` with canonical measurement names, aliases, and known ranges; glossary updates happen automatically during import runs.
- Unknown glossary names go through a second-pass validator prompt that enforces english-only canonical names and aliases before entries are accepted.
- Starred dashboard measurements are saved under `localStorage` key `vitals.starred.measurements`; starred names are bold and sorted to the top of the table.
- Dashboard table cells now render reference ranges visually with a track, min/max bounds, and the observed value marker when numeric range data is available.
