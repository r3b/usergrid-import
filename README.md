usergrid-import
===============

###Usage

node index.js -o YOURORG -a YOURAPP -f data.json

the script takes a stream of json objects, one per line, and sends them to usergrid. If a uuid is present, the method will be PUT, otherwise it will be POST.
