use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::sync::atomic::{AtomicU32, Ordering};

struct Collation;
impl Collation {
    fn stream() -> impl Stream<Item = Option<String>> {
        stream! {
            for c in vec![
                None,
                Some("utf8mb4_unicode_ci".to_owned()),
                Some("utf8mb4_general_ci".to_owned()),
                Some("utf8mb4_bin".to_owned()),
            ]{
                yield c;
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ColumnType {
    Int,
    String(Option<String>), // collation
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Datum {
    Int(i64),
    String(String),
}

impl Datum {
    fn new(column_type: &ColumnType) -> Self {
        match column_type {
            ColumnType::Int => Datum::Int(10),
            ColumnType::String(_) => Datum::String("hello".to_owned()),
        }
    }

    fn next(self) -> Self {
        match self {
            Datum::Int(x) => Datum::Int(x + 1),
            Datum::String(x) => Datum::String(format!("{} x", x)),
        }
    }
}

impl ToString for Datum {
    fn to_string(&self) -> String {
        match self {
            Datum::Int(x) => x.to_string(),
            Datum::String(x) => x.to_string(),
        }
    }
}

impl ColumnType {
    fn new() -> Self {
        ColumnType::Int
    }

    fn stream() -> impl Stream<Item = ColumnType> {
        stream! {
            yield ColumnType::Int;
            let collation_stream = Collation::stream();
            pin_mut!(collation_stream);
            while let Some(c) = collation_stream.next().await {
                yield ColumnType::String(c);
            }
            // let mut column_type = ColumnType::new();
            // loop {
            //     yield column_type.clone();
            //     column_type = match column_type {
            //         ColumnType::Int => Some(ColumnType::String(Collation::new())),
            //         ColumnType::String(collation) => Some(ColumnType::String(collation.next()?)),
            //     }
            // }
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
    // fn new() -> Self {
    //     Column {
    //         name: rand_name("c"),
    //         column_type: ColumnType::Int,
    //     }
    // }

    // fn next(self) -> Option<Self> {
    //     Some(Column {
    //         name: self.name,
    //         column_type: self.column_type.next()?,
    //     })
    // }

    fn stream() -> impl Stream<Item = Column> {
        stream! {
            let name = rand_name("c");
            let column_type_stream = ColumnType::stream();
            pin_mut!(column_type_stream);
            while let Some(column_type) = column_type_stream.next().await {
                yield Column {
                    name: name.clone(),
                    column_type,
                };
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IndexColumn {
    name: String,
    length: Option<u32>,
}

impl IndexColumn {
    // fn new() -> Self {
    //     IndexColumn {
    //         // manually set
    //         name: "unexpected".to_owned(),
    //         length: None,
    //     }
    // }

    // fn next(self) -> Option<Self> {
    //     match self.length {
    //         None => Some(IndexColumn {
    //             name: self.name,
    //             length: Some(3),
    //         }),
    //         Some(_) => None,
    //     }
    // }

    fn stream(col_names: Vec<String>) -> impl Stream<Item = IndexColumn> {
        stream! {
            for name in col_names {
                for length in vec![None, Some(3)] {
                    yield IndexColumn {
                        name: name.clone(),
                        length: length.clone(),
                    };
                }
            }
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

    // fn new() -> Self {
    //     Uniqueness::NonUnique
    // }

    // fn next(self) -> Option<Self> {
    //     match self {
    //         Uniqueness::NonUnique => Some(Uniqueness::Unique),
    //         Uniqueness::Unique => Some(Uniqueness::ClusterdPrimary),
    //         Uniqueness::ClusterdPrimary => Some(Uniqueness::NonClusteredPrimary),
    //         Uniqueness::NonClusteredPrimary => None,
    //     }
    // }

    fn stream() -> impl Stream<Item = Uniqueness> {
        stream! {
            yield Uniqueness::NonUnique;
            yield Uniqueness::Unique;
            yield Uniqueness::ClusterdPrimary;
            yield Uniqueness::NonClusteredPrimary;
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Index {
    name: String,
    columns: Vec<IndexColumn>,
    unique: Uniqueness,
}

impl Index {
    // fn new() -> Self {
    //     Index {
    //         name: rand_name("i"),
    //         // manually set
    //         columns: vec![IndexColumn::new(), IndexColumn::new()],
    //         unique: Uniqueness::new(),
    //     }
    // }

    // fn next(self) -> Option<Self> {
    //     let new_columns = self.columns.clone().next();
    //     match (new_columns, self.unique.next()) {
    //         (Some(new_columns), _) => Some(Index {
    //             name: self.name,
    //             columns: new_columns,
    //             unique: self.unique,
    //         }),
    //         (None, Some(unique)) => Some(Index {
    //             name: self.name,
    //             columns: Next::new(),
    //             unique,
    //         }),
    //         (None, None) => None,
    //     }
    // }

    fn stream(col_names: Vec<String>) -> impl Stream<Item = Index> {
        stream! {
            let name = rand_name("i");
            let c1_stream = IndexColumn::stream(col_names.clone());
            pin_mut!(c1_stream);

            while let Some(c1) = c1_stream.next().await {
                let c2_stream = IndexColumn::stream(col_names.clone());
                pin_mut!(c2_stream);

                while let Some(c2) = c2_stream.next().await {
                    let unique_stream = Uniqueness::stream();
                    pin_mut!(unique_stream);
                    while let Some(unique) = unique_stream.next().await {
                        yield Index {
                            name: name.clone(),
                            columns: vec![c1.clone(), c2.clone()],
                            unique,
                        };
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    cols: Vec<Column>,
    indices: Vec<Index>,
}

impl Table {
    // fn new() -> Self {
    //     let mut t = Table {
    //         name: rand_name("t"),
    //         cols: vec![Column::new(), Column::new()],
    //         indices: vec![Index::new(), Index::new()],
    //     };
    //     t.set_index_column_names();
    //     t
    // }

    // gives an valid Table
    // fn next(mut self) -> Option<Self> {
    // We need to make some modifications to satisfy constraints, e.g.
    // there can be at most 1 primary index, and columns in indices must exist

    // loop {
    //     match self.indices.next() {
    //         Some(x) => {
    //             self.name = rand_name("t");
    //             self.indices = x;
    //             self.set_index_column_names();
    //         }
    //         None => match self.cols.next() {
    //             Some(x) => {
    //                 self.name = rand_name("t");
    //                 self.indices = <Vec<Index> as Next>::new();
    //                 self.cols = x;
    //                 self.set_index_column_names();
    //             }
    //             None => return None,
    //         },
    //     };
    //     if self.constraint_satisfied() {
    //         return Some(self);
    //     }
    // }
    // }

    fn stream() -> impl Stream<Item = Table> {
        stream! {
            let name = rand_name("t");
            let c1_stream = Column::stream();
            pin_mut!(c1_stream);

            while let Some(c1) = c1_stream.next().await {
                let c2_stream = Column::stream();
                pin_mut!(c2_stream);

                while let Some(c2) = c2_stream.next().await {
                    let i1 = Index::stream(vec![c1.name.clone(), c2.name.clone()]);
                    pin_mut!(i1);
        
                    while let Some(i1) = i1.next().await {
                        let i2 = Index::stream(vec![c1.name.clone(), c2.name.clone()]);
                        pin_mut!(i2);

                        while let Some(i2) = i2.next().await {
                            yield Table {
                                name: name.clone(),
                                cols: vec![c1.clone(), c2.clone()],
                                indices: vec![i1.clone(), i2.clone()],
                            };
                        }
                    }
                }
            }
        }
    }
}

pub struct Row {
    cols: Vec<Datum>,
}

impl Row {
    fn new(cols: &[Column]) -> Self {
        Row {
            cols: cols.iter().map(|c| Datum::new(&c.column_type)).collect(),
        }
    }

    pub fn next(self) -> Self {
        Row {
            cols: self
                .cols
                .into_iter()
                .map(|c| c.next())
                .collect::<Vec<Datum>>(),
        }
    }
}

impl ToString for Row {
    fn to_string(&self) -> String {
        self.cols
            .iter()
            .map(|c| match c {
                Datum::Int(i) => i.to_string(),
                Datum::String(s) => format!("\'{}\'", s),
            })
            .collect::<Vec<String>>()
            .join(",")
    }
}

impl Table {
    pub fn create_statement(&self) -> String {
        let mut statement = String::new();
        statement.push_str("CREATE TABLE ");
        statement.push_str(&self.name);
        statement.push_str(" (");
        for (i, col) in self.cols.iter().enumerate() {
            if i > 0 {
                statement.push_str(", ");
            }
            statement.push_str(&col.name);
            statement.push(' ');
            match &col.column_type {
                ColumnType::Int => statement.push_str("INT"),
                ColumnType::String(collation) => {
                    statement.push_str("VARCHAR(10)");
                    if let Some(collation) = collation {
                        statement.push_str(" COLLATE ");
                        statement.push_str(collation);
                    }
                }
            }
        }
        for index in &self.indices {
            statement.push_str(", ");
            match index.unique {
                Uniqueness::NonUnique => {}
                Uniqueness::Unique => statement.push_str("UNIQUE "),
                _ => statement.push_str("PRIMARY "),
            }
            statement.push_str("KEY ");
            statement.push_str(&index.name);
            statement.push_str(" (");
            for (j, col) in index.columns.iter().enumerate() {
                if j > 0 {
                    statement.push_str(", ");
                }
                statement.push_str(&col.name);
                if let Some(length) = col.length {
                    statement.push('(');
                    statement.push_str(&length.to_string());
                    statement.push(')');
                }
            }
            statement.push(')');
            if matches!(&index.unique, Uniqueness::ClusterdPrimary) {
                statement.push_str(" CLUSTERED");
            }
        }
        statement.push(')');
        statement
    }

    pub fn drop_statement(&self) -> String {
        format!("DROP TABLE IF EXISTS {}", &self.name)
    }

    pub fn new_row(&self) -> Row {
        Row::new(&self.cols)
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

#[tokio::test]
async fn generate_table() {
    let table_stream = Table::stream();
    pin_mut!(table_stream);
    let mut cnt = 0;
    while let Some(t) = table_stream.next().await {
        if t.constraint_satisfied() {
            println!("{}", t.create_statement());
            cnt += 1;
        }
    }
    println!("{}", cnt);
}
