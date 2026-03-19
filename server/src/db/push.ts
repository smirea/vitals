import { PROJECT_ROOT } from 'scripts/project-paths.ts';

export async function pushDatabaseSchema() {
    await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(PROJECT_ROOT);
}
