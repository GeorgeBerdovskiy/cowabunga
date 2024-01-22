use std::marker::PhantomData;
use pyo3::prelude::*;

/// Represents record IDs (RIDs). This struct may not be necessary, but improves documentation
#[pyclass]
struct RID {
    /// Internal representation of an RID as a 64-bit signed integer
    _rid: i64
}

/// Represents a table.
#[pyclass]
struct Table {
    /// Represents the indirection column. Every RID points to the next most recent record, and
    /// updated records will usually be found in two or three hops.
    indirection: Vec<RID>
}

impl Table {
    /// Create a new empty table
    pub fn new() -> Self {
        Table { indirection: Vec::new() }
    }
}

/// Empty struct that represents a base page when included as a generic type argument to `Page<T>`.
#[derive(Debug)]
struct Base();

/// Empty struct that represents a tail page when included as a generic type argument to `Page<T>`.
#[derive(Debug)]
struct Tail();

/// Represents either a base or tail page.
/// 
/// Since both are _physically_ the same, we distinguish them using the generic type
/// parameter `T`, which can either be `Base` or `Tail`.
/// 
/// Ideally, implementations for `Page<Base>` and `Page<Tail>` will be different to
/// improve code readability and prevent improper page usage.
struct Page<T> {
    phantom: PhantomData<T>
}

/// Represents a single column, which has an index, a set of base records, and a set of base pages.
/// 
/// This representation may be incorrect. If it is, expect changes soon.
struct Column {
    index: i64,
    base_records: Vec<Page<Base>>,
    tail_records: Vec<Page<Tail>>
}