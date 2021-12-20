use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;

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

impl ToString for ColumnType {
    fn to_string(&self) -> String {
        match self {
            ColumnType::Int => "INT".to_owned(),
            ColumnType::String(Some(c)) => {
                format!("VARCHAR(10) COLLATE {}", c)
            }
            ColumnType::String(None) => "VARCHAR(10)".to_owned(),
        }
    }
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
    fn stream() -> impl Stream<Item = ColumnType> {
        stream! {
            yield ColumnType::Int;
            let collation_stream = Collation::stream();
            pin_mut!(collation_stream);
            while let Some(c) = collation_stream.next().await {
                yield ColumnType::String(c);
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Column {
    name: String,
    column_type: ColumnType,
}

impl Column {
    fn stream(name: String) -> impl Stream<Item = Column> {
        stream! {
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
    fn stream(col_names: Vec<String>) -> impl Stream<Item = IndexColumn> {
        stream! {
            for name in col_names {
                for length in [None, Some(3)] {
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
    fn stream(name: String, col_names: Vec<String>) -> impl Stream<Item = Index> {
        stream! {
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

pub struct Row {
    cols: Vec<Datum>,
}

impl Row {
    fn new(cols: &[Column]) -> Self {
        Row {
            cols: cols.iter().map(|c| Datum::new(&c.column_type)).collect(),
        }
    }

    #[must_use]
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
        let col_clauses = self
            .cols
            .iter()
            .map(|c| format!("{} {}", c.name, c.column_type.to_string()));
        let index_clauses = self.indices.iter().map(|i| {
            format!(
                "{} KEY {} ({}){}",
                match i.unique {
                    Uniqueness::NonUnique => "",
                    Uniqueness::Unique => "UNIQUE",
                    _ => "PRIMARY",
                },
                i.name,
                i.columns
                    .iter()
                    .map(|c| format!(
                        "{}{}",
                        c.name,
                        c.length
                            .map(|l| format!("({})", l))
                            .unwrap_or_else(|| "".to_owned())
                    ))
                    .collect::<Vec<String>>()
                    .join(", "),
                match i.unique {
                    Uniqueness::ClusterdPrimary => " CLUSTERED",
                    _ => "",
                }
            )
        });
        format!(
            "CREATE TABLE {} ({})",
            self.name,
            col_clauses
                .chain(index_clauses)
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    pub fn drop_statement(&self) -> String {
        format!("DROP TABLE IF EXISTS {}", &self.name)
    }

    pub fn new_row(&self) -> Row {
        Row::new(&self.cols)
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

    // provides some *manually constructed* conditions to reduce the number of possible tables
    fn optimized(&self) -> bool {
        if self.indices[0].columns[0].name != "c1" {
            return false;
        }
        if self.indices[0].columns[1].name != "c2" {
            return false;
        }
        if self.indices[1].columns[0].name != "c2" {
            return false;
        }
        if self.indices[1].columns[1].name != "c1" {
            return false;
        }
        true
    }

    pub fn stream() -> impl Stream<Item = Table> {
        stream! {
            let mut table_count = 0;
            let c1_stream = Column::stream("c1".to_owned());
            pin_mut!(c1_stream);

            while let Some(c1) = c1_stream.next().await {
                let c2_stream = Column::stream("c2".to_owned());
                pin_mut!(c2_stream);

                while let Some(c2) = c2_stream.next().await {
                    let i1 = Index::stream("i1".to_owned(), vec![c1.name.clone(), c2.name.clone()]);
                    pin_mut!(i1);

                    while let Some(i1) = i1.next().await {
                        let i2 = Index::stream("i2".to_owned(), vec![c1.name.clone(), c2.name.clone()]);
                        pin_mut!(i2);

                        while let Some(i2) = i2.next().await {
                            let t = Table {
                                name: format!("t{}", table_count),
                                cols: vec![c1.clone(), c2.clone()],
                                indices: vec![i1.clone(), i2.clone()],
                            };
                            if t.constraint_satisfied() && t.optimized(){
                                yield t;
                                table_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
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
