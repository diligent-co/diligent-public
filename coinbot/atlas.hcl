data "external_schema" "gorm" {
  program = [
    "go",
    "run",
    "./atlas/loader",
  ]
}

data "external" "db_config" {
  program = [
    "python", "scripts/parse_config.py", "${atlas.env}"
  ]
}

locals {
  config_file = "config.${atlas.env}.yaml"
  db_config = jsondecode(data.external.db_config)
}

env "local" {
  src = data.external_schema.gorm.url
  dev = "docker+postgres://pgvector/pgvector:pg16/dev?search_path=public" # DB used just to verify the generated SQL schema
  url = "postgres://${local.db_config["user"]}:${local.db_config["password"]}@${local.db_config["host"]}:${local.db_config["port"]}/${local.db_config["database"]}?search_path=public&sslmode=disable"
  migration {
    dir = "file://atlas/migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}

env "prod" {
  src = data.external_schema.gorm.url
  url = "postgres://${local.db_config["user"]}:${local.db_config["password"]}@${local.db_config["host"]}:${local.db_config["port"]}/${local.db_config["database"]}?search_path=public&sslmode=disable"
  migration {
    dir = "file://atlas/migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}

