import { spawnSync } from 'node:child_process';

const simulatorName = 'iPhone 15 Pro';
const commandEnv = {
	...process.env,
	DEVELOPER_DIR: process.env.DEVELOPER_DIR ?? '/Applications/Xcode.app/Contents/Developer',
};

function read(command, args) {
	const result = spawnSync(command, args, {
		encoding: 'utf8',
		env: commandEnv,
	});
	if (result.error) {
		throw result.error;
	}
	if (result.status !== 0) {
		throw new Error(
			[result.stderr?.trim(), `${command} ${args.join(' ')} exited with ${result.status}`]
				.filter(Boolean)
				.join('\n'),
		);
	}
	return result.stdout;
}

function run(command, args) {
	const result = spawnSync(command, args, {
		env: commandEnv,
		stdio: 'inherit',
	});
	if (result.error) {
		throw result.error;
	}
	if (result.status !== 0) {
		throw new Error(`${command} ${args.join(' ')} exited with ${result.status}`);
	}
}

function runtimeScore(runtime) {
	const match = runtime.match(/iOS-(\d+)-(\d+)/);
	if (!match) {
		return 0;
	}
	return Number(match[1]) * 100 + Number(match[2]);
}

function findSimulator() {
	const payload = JSON.parse(read('xcrun', ['simctl', 'list', '--json', 'devices', 'available']));
	const candidates = Object.entries(payload.devices ?? {}).flatMap(([runtime, devices]) =>
		devices
			.filter(device => device.name === simulatorName && device.isAvailable !== false)
			.map(device => ({ ...device, runtime, score: runtimeScore(runtime) })),
	);
	candidates.sort((a, b) => b.score - a.score);
	const [selected] = candidates;
	if (!selected) {
		const names = [
			...new Set(
				Object.values(payload.devices ?? {})
					.flat()
					.filter(device => device.isAvailable !== false)
					.map(device => device.name),
			),
		].sort();
		throw new Error(
			[
				`No available ${simulatorName} simulator found.`,
				'Install it in Xcode > Settings > Platforms, then run this again.',
				`Available simulators: ${names.join(', ') || 'none'}`,
			].join('\n'),
		);
	}
	return selected;
}

try {
	const simulator = findSimulator();
	run('defaults', ['write', 'com.apple.iphonesimulator', 'CurrentDeviceUDID', simulator.udid]);
	const expo = spawnSync('expo', ['start', '--ios'], {
		env: commandEnv,
		stdio: 'inherit',
	});
	if (expo.error) {
		throw expo.error;
	}
	process.exit(expo.status ?? 1);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
