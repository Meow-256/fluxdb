export interface VeloxDBOptions {
    host?: string;
    port?: number;
    password?: string;
    table?: string;
}

export type MeowDBOptions = VeloxDBOptions;

export declare class VeloxDB {
    private host;
    private port;
    private password?;
    private defaultTable;
    private socket;
    private buffer;
    private pendingCallbacks;
    constructor(options?: VeloxDBOptions);
    connect(): Promise<void>;
    private processBuffer;
    send(command: string): Promise<any>;
    set(key: string, value: any, table?: string): Promise<boolean>;
    get<T = any>(key: string, table?: string): Promise<T | null>;
    jsonSet(key: string, path: string, value: any, table?: string): Promise<boolean>;
    top(path: string, limit?: number, table?: string): Promise<any[]>;
    count(query?: string, table?: string): Promise<number>;
    delete(key: string, table?: string): Promise<boolean>;
    close(): void;
}

export declare const MeowDB: typeof VeloxDB;
export declare const FluxDB: typeof VeloxDB;
export default FluxDB;
