import { createFileRoute } from '@tanstack/react-router'
import { z } from 'zod'

import { PillsPage } from '../features/pills/PillsPage'

const pillsSearchSchema = z.object({
    edit: z.coerce.number().int().positive().optional(),
})

export const Route = createFileRoute('/pills')({
    validateSearch: search => pillsSearchSchema.parse(search),
    component: PillsRouteComponent,
})

function PillsRouteComponent() {
    const search = Route.useSearch()
    const navigate = Route.useNavigate()

    return (
        <PillsPage
            editPillId={search.edit ?? null}
            onEditPillChange={editPillId =>
                navigate({
                    search: editPillId ? { edit: editPillId } : {},
                })}
        />
    )
}
