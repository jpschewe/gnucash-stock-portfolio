GnuCash Stock Portfolio Utilities
=================================

Setup your Python virtual environment

    sudo apt-get install python-gnucash
    mkvirtualenv --system-site-packages gnucash-stock-portfolio
    pip install -r requirements.txt


portfolio.py
------------

[GnuCash][GnuCash] Python helper script to add stock quotes (commodity prices) from online sources.
GnuCash already supports online quotes, but these are not implemented for all stock exchanges (e.g. Yahoo Finance does not work with German bonds).
Instead of hacking Perl and/or changing the C source of GnuCash I'm using the GnuCash backend as an API from Python.

get_quotes.py
-------------

Python helper script to add stock quotes like executing `gnucash --add-price-quotes` using python. 
I found that calling gnucash to do this would hang for some unknown reason. However using the python API, everything is fine.


[GnuCash]:        http://www.gnucash.org
