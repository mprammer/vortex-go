// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
// Independently authored legacy-layout coverage through the current Rust API.
use std::{
    env,
    fs::{self, File},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        DeserializeMetadata, IntoArray, VortexSessionExecute,
        arrays::{BoolArray, PrimitiveArray, StructArray, struct_::StructArrayExt},
        dtype::FieldPath,
        expr::stats::Stat,
        stats::as_stat_bitset_bytes,
        validity::Validity,
    },
    error::VortexResult,
    file::{OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::{
        LayoutBuildContext, LayoutRef, LayoutStrategy, LayoutWriterContext, VTable,
        layout_children,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            table::TableStrategy,
            zoned::{LegacyStats, LegacyStatsMetadata},
        },
        segments::SegmentSinkRef,
        sequence::{SendableSequentialStream, SequencePointer, SequentialArrayStreamExt},
    },
    session::{VortexSession, registry::ReadContext},
};

const ZONE: usize = 257;
const ROWS: usize = 2 * ZONE + 7;

fn valid(row: usize) -> bool {
    (row < ZONE && row % 17 != 4) || (row >= 2 * ZONE && row != ROWS - 2)
}
fn value(row: usize) -> i64 {
    if row < ZONE {
        -900 + row as i64 * 3
    } else {
        (1_i64 << 53) + 17 + (row - 2 * ZONE) as i64 * 11
    }
}
fn stats(truncated: bool) -> VortexResult<StructArray> {
    let zones: Vec<Vec<i64>> = (0..ROWS)
        .step_by(ZONE)
        .map(|start| {
            (start..ROWS.min(start + ZONE))
                .filter(|&row| valid(row))
                .map(value)
                .collect()
        })
        .collect();
    StructArray::try_new(
        [
            "max",
            "max_is_truncated",
            "min",
            "min_is_truncated",
            "null_count",
        ]
        .into(),
        vec![
            PrimitiveArray::from_option_iter(zones.iter().map(|v| v.iter().max().copied()))
                .into_array(),
            BoolArray::from_iter([truncated; 3]).into_array(),
            PrimitiveArray::from_option_iter(zones.iter().map(|v| v.iter().min().copied()))
                .into_array(),
            BoolArray::from_iter([truncated; 3]).into_array(),
            PrimitiveArray::from_option_iter(
                zones
                    .iter()
                    .enumerate()
                    .map(|(zone, v)| Some((ZONE.min(ROWS - zone * ZONE) - v.len()) as u64)),
            )
            .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
}

struct IndependentStats {
    truncated: bool,
}

impl LayoutStrategy for IndependentStats {
    // Match the boxed-future ABI of the official async LayoutStrategy trait
    // without adding a generator-only macro dependency.
    fn write_stream<'life0, 'life1, 'async_trait>(
        &'life0 self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        mut eof: SequencePointer,
        session: &'life1 VortexSession,
    ) -> Pin<Box<dyn Future<Output = VortexResult<LayoutRef>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let read_ctx = ReadContext::new(ctx.array_ctx().to_ids());
            let data = ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())
                .write_stream(
                    ctx.clone(),
                    Arc::clone(&segment_sink),
                    stream,
                    eof.split_off(),
                    session,
                )
                .await?;
            assert_eq!(data.row_count(), ROWS as u64);
            let zones = FlatLayoutStrategy::default()
                .write_stream(
                    ctx,
                    segment_sink,
                    stats(self.truncated)?
                        .into_array()
                        .to_array_stream()
                        .sequenced(eof.split_off()),
                    eof,
                    session,
                )
                .await?;
            let dtype = data.dtype().clone();
            let children = layout_children(vec![data, zones]);
            // This is the official legacy wire metadata, authored from the
            // selected statistics, not rewritten from any archived file.
            let mut bytes = (ZONE as u32).to_le_bytes().to_vec();
            bytes.extend(as_stat_bitset_bytes(&[
                Stat::Max,
                Stat::Min,
                Stat::NullCount,
            ]));
            let metadata = LegacyStatsMetadata::deserialize(&bytes)?;
            Ok(<LegacyStats as VTable>::build(
                &LegacyStats,
                &dtype,
                ROWS as u64,
                &metadata,
                vec![],
                children.as_ref(),
                &LayoutBuildContext {
                    session,
                    array_read_ctx: &read_ctx,
                },
            )?
            .into_layout())
        })
    }
}

fn check(
    path: &Path,
    session: &VortexSession,
    rt: &CurrentThreadRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = session.open_options().open_buffer(fs::read(path)?)?;
    let fields = file.footer().layout().children()?;
    assert_eq!(fields.len(), 3);
    for field in &fields[1..] {
        assert_eq!(field.encoding_id().as_ref(), "vortex.stats");
        assert_eq!(field.metadata(), [1, 1, 0, 0, 0x58, 0]);
        let children = field.children()?;
        assert_eq!(children[0].row_count(), ROWS as u64);
        assert_eq!(children[1].row_count(), 3);
        assert_eq!(children[0].children()?.len(), 3);
    }
    let mut ctx = session.create_execution_ctx();
    let mut rows = 0;
    for batch in file.scan()?.into_array_iter(rt)? {
        let batch = batch?.execute::<StructArray>(&mut ctx)?;
        for column in 0..3 {
            let a = batch
                .unmasked_field(column)
                .clone()
                .execute::<PrimitiveArray>(&mut ctx)?;
            let validity = a.validity()?;
            for i in 0..a.len() {
                let row = rows + i;
                let expected_valid = column == 0 || valid(row);
                assert_eq!(validity.execute_is_valid(i, &mut ctx)?, expected_valid);
                if expected_valid {
                    assert_eq!(
                        a.as_slice::<i64>()[i],
                        if column == 0 { row as i64 } else { value(row) }
                    );
                }
            }
        }
        rows += batch.len();
    }
    assert_eq!(rows, ROWS);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory"),
    );
    fs::create_dir_all(&out)?;
    let rt = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(rt.handle());
    let flat = Arc::new(FlatLayoutStrategy::default());
    let strategy = Arc::new(
        TableStrategy::new(
            flat,
            Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
        )
        .with_field_writer(
            FieldPath::from_name("exact"),
            Arc::new(IndependentStats { truncated: false }),
        )
        .with_field_writer(
            FieldPath::from_name("inexact"),
            Arc::new(IndependentStats { truncated: true }),
        ),
    );
    let chunks = (0..ROWS)
        .step_by(ZONE)
        .map(|start| {
            let rows = start..ROWS.min(start + ZONE);
            let values = PrimitiveArray::from_option_iter(
                rows.clone().map(|row| valid(row).then(|| value(row))),
            )
            .into_array();
            StructArray::try_new(
                ["row_id", "exact", "inexact"].into(),
                vec![
                    PrimitiveArray::from_iter(rows.clone().map(|row| row as i64)).into_array(),
                    values.clone(),
                    values,
                ],
                rows.len(),
                Validity::NonNullable,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let path = out.join("owned_legacy_stats.vortex");
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .blocking(&rt)
        .writer(File::create(&path)?, chunks[0].dtype().clone());
    for chunk in chunks {
        writer.push(chunk.into_array())?;
    }
    writer.finish()?;
    check(&path, &session, &rt)?;
    println!(
        "{}: Rust verified {ROWS} original rows, three legacy zones, exact/inexact bounds",
        path.display()
    );
    Ok(())
}
