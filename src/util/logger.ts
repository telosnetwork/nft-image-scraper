import { Logger, pino } from 'pino'
const devMode = process.env.MODE == 'dev'

export function createLogger(source: string, logLevel: string = "info") : Logger {
    console.log(`Creating logger for ${source} ${devMode ? ' in dev mode ' : ''} with level ${logLevel}`)
    const options = {
        level: logLevel,
        name: source,
        transport: {
            target: 'pino-pretty',
            options: {
                colorize: true
            }
        }
    }
    return pino(options)
}
