import type { VitalsDatabase } from 'server/db/client.ts';
import {
    bloodworkMarkers,
    bloodworkReports,
    bloodworkResults,
} from 'server/db/schema.ts';

export function getBloodworkDashboard(db: VitalsDatabase) {
    const reports = db.select().from(bloodworkReports)
        .orderBy(bloodworkReports.date, bloodworkReports.id)
        .all()
        .reverse();

    const markers = db.select().from(bloodworkMarkers)
        .orderBy(bloodworkMarkers.name, bloodworkMarkers.id)
        .all();

    const results = db.select().from(bloodworkResults)
        .orderBy(bloodworkResults.reportId, bloodworkResults.sortOrder, bloodworkResults.id)
        .all();

    return { reports, markers, results };
}
