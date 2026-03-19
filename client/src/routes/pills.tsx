import { createFileRoute } from '@tanstack/react-router'

import { PillsPage } from '../features/pills/PillsPage'

export const Route = createFileRoute('/pills')({
    component: PillsRouteComponent,
})

function PillsRouteComponent() {
    return <PillsPage />
}
