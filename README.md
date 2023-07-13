# DocDB

Document-oriented database as a extension over LevelDB for C++ version 20

## How to add to your project

The whole library is designed as a header-only library. Just insert the appropriate #include where you need to use the library functionality.

However, you will need a library for LevelDB. (libleveldb-dev)

## Data organization within LevelDB

Data and indexes are stored in collections that are implemented within a single LevelDB instance. Up to 255 collections can be created for a single instance (this is a hard limit). 

Individual collections are identified by a name that is specified as a string. Inside the implementation, a collection is identified by a KeyspaceID (abbreviated KID), which takes values from 0-255. When working with collections over C++, this internal identification is transparent; the programmer works with collections using instances of C++ classes

## Documents

Data that are not keys are called documents, whether they are main documents or auxiliary values in indexes. The programmer specifies the data type for the document representation, but must also define how the document is serialized and deserialized

The definition of serialization functions is done using a C++ class (or structure) and must satisfy the DocumentDef concept. It takes the following form:

```
struct DocumentDefinition {
    using Type = DocumentType;
    template<typename Iter>
    static DocumentType from_binary(Iter beg, Iter end);
    template<typename Iter>
    static Iter to_binary(const DocumentType &row, Iter iter);
};
```
Note that iterators are templates. This is important to note because different parts of the library may use different types of iterators for serialization and deserialization. The only common property of iterators is that they all work with the `char` type

### Pre-prepared document types

* **StringDocument** - The document is of type `std::string`
* **RowDocument** - A document represents a database row that can have multiple columns of different types. More information on the `Row` class
* **FixedRowDocument<>** - An extension to `RowDocument` where we can specify the types of individual columns using template parameters. `FixedRowDocument<int, double, std::string, bool>`
* **StructuredDocument** - A structured document that resembles a javascript object in structure, i.e. an object that can store numbers, strings, fields indexed by order and by string. There is a function for conversion from and to JSON format. See `Structured` class

## Database initialization and database instance

 * The database instance is stored in a variable of type `docdb::PDatabase`. 
 * Databázi inicializujeme pomocí funkce: `docdb::Database::create(path,options)`. The `path` parameter selects the directory where the database is or will be stored. The options parameter is described in the LevelDB library documentation
 
```
static docdb::PDatabase open_db() {
    leveldb::Options opts;
    opts.create_if_missing = true;
    return docdb::Database::create("example_db", opts);
}
```

A variable of type `PDatabase` then is used as a parameter into all collection instances that may appear in the program. Each instance takes one reference. In order to successfully close the database, all references to the database must be cleared

## Types of collections

### Storage

`Storage` is used for unorganized storage of documents and is used as a data source. This collection is also often the only collection that is directly written to. Other collections are changed based on changes made to this collection.

It is recommended to create an alias to a specific configuration of the Storage class using the `using` keyword, because it is declared as a template in the library

```
using Storage = docdb::Storage<DocumentType>;
```

... where `DocumentDef` is the document definition as described above.

Documents are only inserted into the collection (in normal use). Each newly inserted document receives a unique ID, which is represented by the `DocID` type. If an existing document needs to be modified, the modified document is inserted again and the DocID of the document being replaced is given along with the insertion. However, the original document remains in the storage until it is removed manually using the `compact` tool, which the programmer must use from time to time unless he needs to maintain historical revisions of documents

Documents can be removed from the s, but this operation is also implemented by inserting an empty document into the storage, which is presented as a deleted document on the interface. This deposit also has an associated DocID.

Organizing storage in this way makes it easy to back up data, even incrementally, and it is also easy to implement data replication

All documents in the storage must be of the same type, but this does not prevent the use of a variant type. It is then possible (and recommended) to store all document types "in a pile" in a single storage. Sorting documents not only by type but also by other properties is a task for indexers. 

The DocID is not a suitable document identifier because a new ID is generated every time the document changes. That's why document revision identification plays a role here. If your application needs to identify documents using some other type, for example using a string (unique document id), then this is a matter of appropriate design of the document type, and in order to search for documents identified using this unique id, the programmer must attach a unique index to the storage, which indexes these identifiers.

### Index

