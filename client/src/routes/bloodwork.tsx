import { createFileRoute } from '@tanstack/react-router'

import { VitalsDashboard } from '../features/vitals/VitalsDashboard'
import '../features/vitals/vitals.css'

export const Route = createFileRoute('/bloodwork')({
    component: BloodworkPage,
})

function BloodworkPage() {
    return <VitalsDashboard />
}
