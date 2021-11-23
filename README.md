# ckanext-cioos_harvest

This is the CIOOS-SIOOC plugin to modifie the behaviour of the spatial harvester
extension. It's primary purpose is to mosify the dataset package data during
spatial harvest so that it will work with the scheming, composit, repeating,
and fluent extensions.

Currently it adds:
* transform from ISO19115-3 to ISO19115-1 format for harvested metadata to make 19115-3 metadata work with the spatial harvesters default 19115-1 schemas
* during the gather stage of a spatial harvest the data package is modified by
  * moving all scheming fields from extras into the data package root
  * insuring that values of list and multi list scheming fields is entered as a list rather then a string
  * place composit and composit repeating fields into the '__extras' subkey as this is where the extension expects to find them
  * rename composit fields using the seperator found in the config file and colapse nested keys into a concatinated field name
  * convert fluent tag fields into a dictinary of language lists rather then the default list of dictinary languages
  * populate fluent fields with language dictinarys using the default language if no language dictinary is provided

------------
Requirements
------------
Tested on ckan 2.8 but likely works for earlyer versions. This extension requires ckanext-scheming, ckanext-composite, and chanext-fluent to also be installed. If these extensions are missing this code will do very little.

As of ckan 2.9 ckanext-composite requirment has been droped.

config options:

#### CKAN.ini
set timeout of request.get when trying to read full xml body from xml url. Used
in cioos ckan custom harvester
`ckan.index_xml_url_read_timeout=500`

#### Harvester Source Config
set timeout of request.get when trying to read full xml body from xml url. Used
in cioos ckan custom harvester
`'url_read_timeout': 500`

------------
Installation
------------

.. Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-cioos_harvest:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-cioos_harvest Python package into your virtual environment::

     pip install ckanext-cioos_harvest

3. Add ``cioos_harvest`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


-----------------
Running the Tests
-----------------

Sorry, no test at this time
