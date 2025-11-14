import colors from 'colors';

type LogArgs = unknown[];

class Log {
    constructor(private readonly subject: string) {}

    info(...args: LogArgs): void {
        if (!args.length) return;
        const [message, ...rest] = args;
        const entry = this.formatEntry(String(message));
        console.log(entry, ...rest);
    }

    error(...args: LogArgs): void {
        if (!args.length) return;
        const [message, ...rest] = args;
        const entry = colors.red(this.formatEntry(String(message)));

        rest.forEach(arg => {
            if (arg instanceof Error && arg.stack) {
                console.log(arg.stack);
            }
        });

        console.error(entry, ...rest);
    }

    private formatEntry(message: string): string {
        return `[${new Date().toISOString()}] [${this.subject}] ${message}`;
    }
}

export type Logger = ((...args: LogArgs) => void) & { error: (...args: LogArgs) => void };

export function createLogger(subject: string): Logger {
    const logger = new Log(subject);
    const boundInfo = logger.info.bind(logger) as Logger;
    boundInfo.error = logger.error.bind(logger);
    return boundInfo;
}
