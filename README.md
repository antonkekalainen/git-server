# Clojure Git server
## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

You will additionally need a running instance of PostgreSQL, modify the `server.properties` file to point to your server. 
You will also have to run the `pg_init.sql` script against your database. This can be achieved by running `psql git git < pg_init.sql` if your database is called `git` and is owned by user `git`.

## Running

To start a web server for the application, run:

    lein ring server-headless

After which you can add `http://localhost:3000/repo/XXX` as a remote to a repo and push, creating a repository called XXX on the server.

The server will automatically run on port 3000, change this in `project.clj` if you want to. A directory named `pack` will automatically be created in the directory the server is running in, containing the code stored in your repository. 

## License

Copyright © 2022
