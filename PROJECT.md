# Vitals

This is an app to easily track my vitals like bloodwork and wellbeing

Pick the next most important feature to work on, implement it fully and test it if possible, tick it and commit when done. the Notes section is for you to comment on things that are worth remembering, more generic points add them to AGENTS.md. Update this document as requirements change or get clarified if needed

# Features:

- [ ] blodwork importer: converts unstructured varied bloodwork pdfs into standardized json and uploads to s3
    - [x] use createScript.ts framework
    - [x] `scripts/bloodwork-import.ts` accepts a pdf as input, uses AI (gemini 3 flash via ai sdk + openrouter) to convert it to a standardized json template that includes date, location, lab name, import location (optional, flag), weight and the table of all standardized measurements. for each measurement capture value, ranges, flags, notes etc. store all the parsed data as `data/bloodwork_{date}_{lab}.json` pretty json 4 spaces
    - [x] account for the bloodwork being in different languages and vastly different formats output must be standardized and in engligh
    - [x] all data must also be uploaded to s3 in `stefan-life/vitals/bloodwork_{date}_{lab}.json` bucket location
    - [x] all my existing labs are in `data/to-import`, use them for testing and to get a sense of various potential formats. create a standard `BloodworkLab` zod type and use that as the basis for the various tools and enforce the json be in that shape. a lot of the properties will have to be optional most likely
    - [ ] once everything is working and tested, import all data from `data/to-import`
- [ ] data sync: downloads data from s3
    - [ ] create a script to download data from the bucket via `scripts/download-data.ts`
    - [ ] script only downloads what has changed
    - [ ] when the server starts it would automatically call this script
- [ ] historical dashboard: client application to analyze the data
    - [ ] optimized for web, but also usable on mobile
    - [ ] use ant-design components
    - [ ] show a table of every single datapoint (names as rows, source as a column, from latest to oldest). allow filter
    - [ ] allow selecting rows and columns (default to all columns) to see data on a chart, I want to see how the various vitals have trended over time. chart should show to the right of the table always visible if there are items selected
    - [ ] in mobile portrait mode, only show the latest value and allow to switch the column to go to a different lab. checking items should show a chart under the table (chart always visible if values are selected so table takes up less space)
- [ ] use the aws cli to create a purpose built user for this project with dedicated permissions and store the credentials in .env.local
- [ ] create env with env-manager and create specific keys for everything requested (you can use env-manager to generate an openrouter key)

# Notes:
- `data/to-import/2024-06-20_Lab_Results.pdf` is not a valid PDF (signature mismatch), so it cannot currently be imported until the source file is fixed/replaced.
- Importer runtime requires `OPENROUTER_API_KEY` and AWS env vars (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) unless running with `--skip-upload`.
