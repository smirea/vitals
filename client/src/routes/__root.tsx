import { Outlet, createRootRoute } from '@tanstack/react-router';

export const Route = createRootRoute({
    component: function RootComponent() {
        return (
            <main className='flex min-h-screen items-stretch justify-stretch'>
                <Outlet />
            </main>
        );
    },
});
