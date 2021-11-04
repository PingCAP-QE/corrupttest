use std::sync::atomic::{AtomicU32, Ordering};

type Collation = Option<String>;

pub trait Next {
    fn new() -> Self;
    fn next(self) -> Option<Self>
    where
        Self: Sized;
}

impl Next for Collation {
    fn new() -> Self {
        Collation::None
    }

    fn next(self) -> Option<Self> {
        match self {
            None => Some(Some("utf8mb4_unicode_ci".to_owned())),
            Some(x) => match x.as_str() {
                "utf8mb4_unicode_ci" => Some(Some("utf8mb4_general_ci".to_owned())),
                "utf8mb4_general_ci" => Some(Some("utf8mb4_bin".to_owned())),
                "utf8mb4_bin" => None,
                _ => panic!("unexpected collation"),
            },
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ColumnType {
    Int,
    String(Collation),
}

impl Next for ColumnType {
    fn new() -> Self {
        ColumnType::Int
    }

    fn next(self) -> Option<Self> {
        match self {
            ColumnType::Int => Some(ColumnType::String(Collation::new())),
            ColumnType::String(collation) => Some(ColumnType::String(collation.next()?)),
        }
    }
}

static NAME_COUNTER: AtomicU32 = AtomicU32::new(0);

fn rand_name(prefix: &str) -> String {
    format!("{}{}", prefix, NAME_COUNTER.fetch_add(1, Ordering::SeqCst))
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Column {
    name: String,
    column_type: ColumnType,
}

impl Column {
    fn gen_datum(&self) -> String {
        match self.column_type {
            ColumnType::Int => "10".to_owned(),
            ColumnType::String(_) => "1234567890".to_owned(),
        }
    }
}

impl Next for Column {
    fn new() -> Self {
        Column {
            name: rand_name("c"),
            column_type: ColumnType::Int,
        }
    }

    fn next(self) -> Option<Self> {
        Some(Column {
            name: self.name,
            column_type: self.column_type.next()?,
        })
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IndexColumn {
    name: String,
    length: Option<u32>,
}

impl Next for IndexColumn {
    fn new() -> Self {
        IndexColumn {
            // manually set
            name: "unexpected".to_owned(),
            length: None,
        }
    }

    fn next(self) -> Option<Self> {
        match self.length {
            None => Some(IndexColumn {
                name: self.name,
                length: Some(3),
            }),
            Some(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Uniqueness {
    NonUnique,
    Unique,
    ClusterdPrimary,
    NonClusteredPrimary,
}

impl Uniqueness {
    fn is_primary(&self) -> bool {
        matches!(self, Self::ClusterdPrimary | Self::NonClusteredPrimary)
    }
}

impl Next for Uniqueness {
    fn new() -> Self {
        Uniqueness::NonUnique
    }

    fn next(self) -> Option<Self> {
        match self {
            Uniqueness::NonUnique => Some(Uniqueness::Unique),
            Uniqueness::Unique => Some(Uniqueness::ClusterdPrimary),
            Uniqueness::ClusterdPrimary => Some(Uniqueness::NonClusteredPrimary),
            Uniqueness::NonClusteredPrimary => None,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Index {
    name: String,
    columns: Vec<IndexColumn>,
    unique: Uniqueness,
}

impl Next for Index {
    fn new() -> Self {
        Index {
            name: rand_name("i"),
            // manually set
            columns: vec![IndexColumn::new(), IndexColumn::new()],
            unique: Uniqueness::new(),
        }
    }

    fn next(self) -> Option<Self> {
        let new_columns = self.columns.clone().next();
        match (new_columns, self.unique.next()) {
            (Some(new_columns), _) => Some(Index {
                name: self.name,
                columns: new_columns,
                unique: self.unique,
            }),
            (None, Some(unique)) => Some(Index {
                name: self.name,
                columns: Next::new(),
                unique,
            }),
            (None, None) => None,
        }
    }
}

impl<T: Next + Clone> Next for Vec<T> {
    fn new() -> Self {
        // manual setting
        vec![T::new(), T::new()]
    }

    fn next(mut self) -> Option<Self> {
        // Imagine the proecss as an integer increment. Start from the least significant digit.
        for (i, v) in self.iter_mut().enumerate().rev() {
            match v.clone().next() {
                Some(x) => {
                    self[i] = x;
                    return Some(self);
                }
                None => {
                    *v = T::new();
                }
            }
        }

        None
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    cols: Vec<Column>,
    indices: Vec<Index>,
}

impl Next for Table {
    fn new() -> Self {
        let mut t = Table {
            name: rand_name("t"),
            cols: vec![Column::new(), Column::new()],
            indices: vec![Index::new(), Index::new()],
        };
        t.set_index_column_names();
        t
    }

    // gives an valid Table
    fn next(mut self) -> Option<Self> {
        // We need to make some modifications to satisfy constraints, e.g.
        // there can be at most 1 primary index, and columns in indices must exist
        loop {
            match self.indices.next() {
                Some(x) => {
                    self.name = rand_name("t");
                    self.indices = x;
                    self.set_index_column_names();
                }
                None => match self.cols.next() {
                    Some(x) => {
                        self.name = rand_name("t");
                        self.indices = <Vec<Index> as Next>::new();
                        self.cols = x;
                        self.set_index_column_names();
                    }
                    None => return None,
                },
            };
            if self.constraint_satisfied() {
                return Some(self);
            }
        }
    }
}

impl Table {
    pub fn create_sentence(&self) -> String {
        let mut sentence = String::new();
        sentence.push_str("CREATE TABLE ");
        sentence.push_str(&self.name);
        sentence.push_str(" (");
        for (i, col) in self.cols.iter().enumerate() {
            if i > 0 {
                sentence.push_str(", ");
            }
            sentence.push_str(&col.name);
            sentence.push_str(" ");
            match &col.column_type {
                ColumnType::Int => sentence.push_str("INT"),
                ColumnType::String(collation) => {
                    sentence.push_str("VARCHAR(10)");
                    if let Some(collation) = collation {
                        sentence.push_str(" COLLATE ");
                        sentence.push_str(&collation);
                    }
                }
            }
        }
        for index in &self.indices {
            sentence.push_str(", ");
            match index.unique {
                Uniqueness::NonUnique => {}
                Uniqueness::Unique => sentence.push_str("UNIQUE "),
                _ => sentence.push_str("PRIMARY "),
            }
            sentence.push_str("KEY ");
            sentence.push_str(&index.name);
            sentence.push_str(" (");
            for (j, col) in index.columns.iter().enumerate() {
                if j > 0 {
                    sentence.push_str(", ");
                }
                sentence.push_str(&col.name);
                if let Some(length) = col.length {
                    sentence.push_str("(");
                    sentence.push_str(&length.to_string());
                    sentence.push_str(")");
                }
            }
            sentence.push_str(")");
            if matches!(&index.unique, Uniqueness::ClusterdPrimary) {
                sentence.push_str(" CLUSTERED");
            }
        }
        sentence.push_str(")");
        sentence
    }

    pub fn drop_sentence(&self) -> String {
        format!("DROP TABLE IF EXISTS {}", &self.name)
    }

    pub fn gen_row(&self) -> Vec<String> {
        let mut row = Vec::new();
        for col in &self.cols {
            row.push(col.gen_datum());
        }
        row
    }

    // manual setting
    fn set_index_column_names(&mut self) {
        self.indices[0].columns[0].name = self.cols[0].name.clone();
        self.indices[0].columns[1].name = self.cols[1].name.clone();
        self.indices[1].columns[0].name = self.cols[1].name.clone();
        self.indices[1].columns[1].name = self.cols[0].name.clone();
    }

    fn constraint_satisfied(&self) -> bool {
        let mut satisfied = true;

        // at most 1 primary index
        if self
            .indices
            .iter()
            .filter(|x| x.unique.is_primary())
            .count()
            > 1
        {
            satisfied = false;
        }
        if self.indices.iter().any(|x| x.columns.len() > 2) {
            satisfied = false;
        }
        if self.indices.iter().any(|x| {
            x.columns
                .iter()
                .any(|y| !self.cols.iter().any(|z| z.name == y.name))
        }) {
            // sanity check
            unreachable!();
        }

        // prefix index is only for string type
        if self.indices.iter().any(|x| {
            x.columns.iter().any(|y| {
                !matches!(
                    self.cols
                        .iter()
                        .find(|z| z.name == y.name)
                        .unwrap()
                        .column_type,
                    ColumnType::String(_)
                )
            })
        }) {
            satisfied = false;
        }

        satisfied
    }
}

pub struct TableIterator {
    pub table: Option<Table>,
}

impl TableIterator {
    pub fn new() -> Self {
        TableIterator {
            table: Some(Table::new()),
        }
    }
}

impl Iterator for TableIterator {
    type Item = Table;

    fn next(&mut self) -> Option<Self::Item> {
        self.table = self.table.clone().and_then(Next::next);
        self.table.clone()
    }
}
