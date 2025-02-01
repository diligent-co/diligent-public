# Database migrations

This database migration use [Atlas](https://atlasgo.io/guides/orms/gorm) for gorm. The tool helps to generate the difference between actual GORM models and the state of the database after the migrations were applied.

## Usage

### Install atlas cli tool

Run `curl -sSf https://atlasgo.sh | sh` or go to Atlas page more info about cli install.

Once the client is installed, we have different things we would want to do to the Database.

### Create new migration from the actual code.

The actual migration config is configured to be run in [Go program mode](https://atlasgo.io/guides/orms/gorm#go-program-mode). This means that the structs that will generate a migration must be in the [loader](loader/main.go) program.

This files load an array from [interface.go](../src/database/interface.go). In this file we have one array -`GormTables`.

Once the struct is in this array and/or we modified the struct and want to have a new migration, we can run the following atlas cli command in the server folder (where the [atlas.hcl](../atlas.hcl) file lives):

```bash
atlas migrate diff --env local
```

This command will generate a new `sql` file in the [atlas/migrations](migrations/) folder. Here we will have 1 migration file per database modification.

### Changing the migration while developing

If we want to change the migration, we can delete the migration file that was generated.
Run the following to reset the hash in atlas.sum
```bash
atlas migrate hash --env local
```
Re-generate the migrations as though it were need


### Apply the migrations

To apply the migration the following command should be run:

```bash
atlas migrate apply --env local
```

This will apply the migrations in a local database, following the [url](../atlas.hcl:12) field of gorm environment

If the idea is to apply in some other database, the command can be used as follow:

```bash
atlas migrate apply --env gorm --url "postgres://user:pass@server:port/database?search_path=public&sslmode=verify-ca
```

There should be certificates living in the following folders if there is need for ssl encription `sslmode=verify-ca`

| File                         | Contents                        |
| ---------------------------- | ------------------------------- |
| ~/.postgresql/postgresql.crt | client certificate              |
| ~/.postgresql/postgresql.key | client private key              |
| ~/.postgresql/root.crt       | trusted certificate authorities |

Or the url can be generated in the following way:
```
postgres://user:pass@server:port/database?search_path=public&sslmode=verify-ca&sslcert=/path/to/crt&sslkey=/path/to/key&sslrootcert=/path/to/rootca