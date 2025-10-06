import type { DataFrame } from "hightable"
import { arrayDataFrame } from "hightable"

// import type { AsyncBuffer } from './helpers.js';

/**
 * Helpers to load a CSV file as a dataframe
 */
export async function csvDataFrame(): Promise<DataFrame> {
    // TODO(SL): implement CSV to DataFrame conversion using streaming. For now: only download everything, and 

    await Promise.resolve() // simulate async
    const df = arrayDataFrame([{ test: 'a' }, { test: 'b' }]);
    console.log(df)
    return df
}


//  */
// export interface DataFrame<M extends Obj = Obj, C extends Obj = Obj> {
//     numRows: number;
//     columnDescriptors: readonly ColumnDescriptor<C>[];
//     metadata?: M;
//     exclusiveSort?: boolean;
//     getCell({ row, column, orderBy }: {
//         row: number;
//         column: string;
//         orderBy?: OrderBy;
//     }): ResolvedValue | undefined;
//     getRowNumber({ row, orderBy }: {
//         row: number;
//         orderBy?: OrderBy;
//     }): ResolvedValue<number> | undefined;
//     fetch?: Fetch;
//     eventTarget?: CustomEventTarget<DataFrameEvents>;
// }
