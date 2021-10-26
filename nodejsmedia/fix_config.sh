#!/bin/bash

# Reference : https://stackoverflow.com/questions/52998331/imagemagick-security-policy-pdf-blocking-conversion#53180170
echo "Fix ImageMagick policy.xml pour permettre lecture PDF"
cp /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xml.old
sed -i -e 's/rights="none" pattern="PDF"/rights="read" pattern="PDF"/' /etc/ImageMagick-6/policy.xml
