[![CI Pipeline](https://github.com/joefrost01/frameblaze/actions/workflows/ci.yml/badge.svg)](https://github.com/joefrost01/frameblaze/actions/workflows/ci.yml)
[![Quarto Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://joefrost01.github.io/frameblaze/)

# Frameblaze

**Frameblaze** is a CLI tool written in Rust for converting data between various formats and locations, with a basic column filter transformation.

## Documentation

Check out the **[official documentation](https://joefrost01.github.io/frameblaze/)** for a quick start and usage guides.

## Features
- Convert CSV/Parquet/Avro/Arrow ↔ CSV/Parquet/Avro/Arrow.
- Include or exclude columns via CLI flags.
- Extendable architecture to add more formats (Excel, JSON) or transformations later.
- Apache-2.0 licensed.

## Usage

1. **Build and Install**:

```bash
   git clone https://github.com/yourusername/frameblaze.git
   cd frameblaze
   cargo install --path .
```

## Convert a parquet file to CSV
```bash
frameblaze parquet csv input.parquet --output output.csv
```

## Include or exclude columns
```bash
# Keep only columns "name" and "age"
frameblaze parquet csv input.parquet --output filtered.csv --include-columns name,age

# Exclude columns "address" and "phone"
frameblaze parquet csv input.parquet --output filtered.csv --exclude-columns address,phone
```

## Roadmap
* True streaming/chunk-based reading/writing.
* Cloud storage
* More complex transformations (filter rows).
* Config files and environment variable support for all options.

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
Polars for the inspiration and the great work they do in the Rust ecosystem.

Happy blazing through your data frames!
