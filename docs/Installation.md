# Installation Instructions

Suggested approach:

* Install [Redis](https://redis.io/topics/quickstart)
* Download the repository: `git clone https://github.com/danielczech/meerkat-backend-interface`
* Make a virtual environment according to your preferences. 
    * `virtualenv -p <python3> <name>`
    * `source /path/to/virtualenv/<name>/bin/activate`
* Install katversion: `pip install -r requirements.txt`
* Install the `meerkat-backend-interface`:
    * `cd meerkat-backend-interface`
    * `<python3> setup.py install`
