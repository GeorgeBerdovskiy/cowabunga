use pyo3::prelude::*;

use crate::table;

#[derive(Debug, Clone, Copy)]
pub enum QueryName {
    Insert,
    Update,
    Select,
    Sum,
    SelectVersion,
    SumVersion,
    Delete
}

#[derive(Debug, Clone)]
pub struct Query {
    pub query: QueryName,
    pub table: usize,

    pub single_arg_1: Option<i64>,
    pub single_arg_2: Option<i64>,
    pub single_arg_3: Option<i64>,
    pub single_arg_4: Option<i64>,

    pub list_arg: Vec<Option<i64>>,

    pub primary_key_index: usize
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Transaction {
    pub queries: Vec<Query>
}

#[pymethods]
impl Transaction {
    #[new]
    pub fn new() -> Self {
        Transaction {
            queries: Vec::new()
        }
    }

    pub fn add_insert(&mut self, table: usize, primary_key_index: usize, args: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Insert,
            table: table,

            single_arg_1: None,
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: args,

            primary_key_index 
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_update(&mut self, table: usize, primary_key_index: usize, primary_key: i64, args: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Update,
            table: table,

            single_arg_1: Some(primary_key),
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: args,

            primary_key_index
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_select(&mut self, table: usize, primary_key_index: usize, search_key: i64, search_key_index: i64, projected_columns: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Select,
            table: table,

            single_arg_1: Some(search_key),
            single_arg_2: Some(search_key_index),
            single_arg_3: None,
            single_arg_4: None,

            list_arg: projected_columns,
            
            primary_key_index
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_sum(&mut self, table: usize, primary_key_index: usize, start_range: i64, end_range: i64, column_index: i64) {
        self.queries.push(Query {
            query: QueryName::Sum,
            table: table,

            single_arg_1: Some(start_range),
            single_arg_2: Some(end_range),
            single_arg_3: Some(column_index),
            single_arg_4: None,

            list_arg: Vec::new(),

            primary_key_index
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_sum_version(&mut self, table: usize, primary_key_index: usize, start_range: i64, end_range: i64, column_index: i64, relative_version: i64) {
        self.queries.push(Query {
            query: QueryName::SumVersion,
            table: table,

            single_arg_1: Some(start_range),
            single_arg_2: Some(end_range),
            single_arg_3: Some(column_index),
            single_arg_4: Some(relative_version),

            list_arg: Vec::new(),

            primary_key_index
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_select_version(&mut self, table: usize, primary_key_index: usize, search_key: i64, search_key_index: i64, proj: Vec<Option<i64>>, relative_version: i64) {
        self.queries.push(Query {
            query: QueryName::SelectVersion,
            table: table,

            single_arg_1: Some(search_key),
            single_arg_2: Some(search_key_index),
            single_arg_3: Some(relative_version),
            single_arg_4: None,

            list_arg: proj,

            primary_key_index
        });

        //println!("{:?}", self.queries);
    }

    pub fn add_delete(&mut self, table: usize, primary_key_index: usize, primary_key: i64) {
        self.queries.push(Query {
            query: QueryName::Delete,
            table: table,

            single_arg_1: Some(primary_key),
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: Vec::new(),

            primary_key_index
        });

        //println!("{:?}", self.queries);
    }
}