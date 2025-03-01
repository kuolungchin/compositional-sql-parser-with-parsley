# CLAUDE.md - Compositional SQL Parser With Parsley

## Build & Test Commands
- Compile: `sbt compile`
- Run tests: `sbt test`
- Run specific test class: `sbt "testOnly *SimpleSelectParserSpec"`
- Run test with specific name: `sbt "testOnly *SimpleSelectParserSpec -- -t \"test name\""`
- Clean: `sbt clean`
- Run application: `sbt run`

## Code Style Guidelines
- **Naming**: PascalCase for types/objects, camelCase for methods/variables
- **Organization**: Sealed traits with case classes for AST nodes, lazy vals for parsers
- **Imports**: Group by library/package, explicit imports preferred over wildcards
- **Types**: Use sealed traits for hierarchies, case classes for data structures
- **Formatting**: 2-space indent, opening braces on same line, methods chained with dot at line start
- **Error Handling**: Use `Either[String, Result]` pattern for parsing results
- **Documentation**: Comments for complex parsers, test cases as documentation
- **Parser Design**: Small, focused, composable parsers with explicit precedence handling