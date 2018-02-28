#!/usr/bin/env bash

apt-get update
apt-get install -y apache2
if ! [ -L /var/www ]; then
  rm -rf /var/www/html
  ln -fs /vagrant /var/www/html
fi
apt-get install -y php
apt-get install -y libapache2-mod-php
apt-get install -y php-mbstring
apt-get install -y php-xml
apt-get install -y php-curl
apt-get install -y php-zip
apt-get install -y composer
