// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
// Preserve explicit delta storage; original row formulas are the value/null oracle.
use std::{
    env,
    fs::{self, File},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{PrimitiveArray, StructArray, struct_::StructArrayExt},
        validity::Validity,
    },
    editions::{EditionSessionExt, PREVIEW_2026_06_0},
    encodings::fastlanes::{Delta, DeltaArray, DeltaArraySlotsExt},
    file::{OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    session::VortexSession,
};

const ROWS: usize = 2500;
const NAMES: [&str; 6] = ["row_id", "v32", "v64", "required", "all_valid", "all_null"];

fn valid(column: usize, row: usize) -> bool {
    match column {
        1 => row % 3 != 0,
        2 => row % 5 != 2 && !(1001..1041).contains(&row),
        5 => false,
        _ => true,
    }
}
fn value64(row: usize) -> i64 {
    i64::MIN + row as i64 * 19
}
fn sliced(
    array: &DeltaArray,
    range: Range<usize>,
) -> Result<DeltaArray, Box<dyn std::error::Error>> {
    let lanes = if array.dtype().as_ptype().byte_width() == 8 {
        16
    } else {
        32
    };
    let start_chunk = range.start / 1024;
    let stop_chunk = range.end.div_ceil(1024);
    Ok(Delta::try_new(
        array
            .bases()
            .slice(start_chunk * lanes..stop_chunk * lanes)?,
        array
            .deltas()
            .slice(start_chunk * 1024..stop_chunk * 1024)?,
        range.start % 1024,
        range.len(),
    )?)
}
fn check(
    path: &Path,
    range: Range<usize>,
    session: &VortexSession,
    rt: &CurrentThreadRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = session.open_options().open_buffer(fs::read(path)?)?;
    let mut ctx = session.create_execution_ctx();
    let mut rows = 0;
    for batch in file.scan()?.into_array_iter(rt)? {
        let batch = batch?.execute::<StructArray>(&mut ctx)?;
        for (column, name) in NAMES.iter().enumerate() {
            let a = batch
                .unmasked_field(column)
                .clone()
                .execute::<PrimitiveArray>(&mut ctx)?;
            let validity = a.validity()?;
            for i in 0..a.len() {
                let row = range.start + rows + i;
                assert_eq!(
                    validity.execute_is_valid(i, &mut ctx)?,
                    valid(column, row),
                    "{name} row {row} validity"
                );
                if valid(column, row) {
                    match column {
                        0 => assert_eq!(a.as_slice::<i64>()[i], row as i64),
                        2 => assert_eq!(a.as_slice::<i64>()[i], value64(row)),
                        _ => assert_eq!(a.as_slice::<i32>()[i], row as i32),
                    }
                }
            }
        }
        rows += batch.len();
    }
    assert_eq!(rows, range.len());
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory argument or VORTEX_TESTDATA_DIR"),
    );
    fs::create_dir_all(&out)?;
    let rt = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(rt.handle());
    // Delta is a registered preview encoding in the pinned Rust revision.
    session.enable_edition(PREVIEW_2026_06_0)?;
    let mut ctx = session.create_execution_ctx();
    let source = [
        PrimitiveArray::from_option_iter((0..ROWS).map(|row| valid(1, row).then_some(row as i32))),
        PrimitiveArray::from_option_iter((0..ROWS).map(|row| valid(2, row).then(|| value64(row)))),
        PrimitiveArray::from_iter((0..ROWS).map(|row| row as i32)),
        PrimitiveArray::from_option_iter((0..ROWS).map(|row| Some(row as i32))),
        PrimitiveArray::from_option_iter((0..ROWS).map(|_| None::<i32>)),
    ];
    let arrays = source
        .iter()
        .map(|a| Delta::try_from_primitive_array(a, &mut ctx))
        .collect::<Result<Vec<_>, _>>()?;
    for (suffix, range) in [
        ("", 0..ROWS),
        ("_slice", 1000..1050),
        ("_late_slice", 1023..2051),
        ("_tail", 2400..ROWS),
    ] {
        let mut fields: Vec<ArrayRef> =
            vec![PrimitiveArray::from_iter(range.clone().map(|row| row as i64)).into_array()];
        for array in &arrays {
            fields.push(sliced(array, range.clone())?.into_array());
        }
        let table = StructArray::try_new(NAMES.into(), fields, range.len(), Validity::NonNullable)?;
        let flat = Arc::new(FlatLayoutStrategy::default());
        let strategy = Arc::new(TableStrategy::new(flat.clone(), flat));
        let path = out.join(format!("delta_nullable{suffix}.vortex"));
        let mut writer = session
            .write_options()
            .with_strategy(strategy)
            .blocking(&rt)
            .writer(File::create(&path)?, table.dtype().clone());
        writer.push(table.into_array())?;
        writer.finish()?;
        check(&path, range.clone(), &session, &rt)?;
        println!(
            "{}: Rust reopened and verified source rows {range:?}",
            path.display()
        );
    }
    Ok(())
}
