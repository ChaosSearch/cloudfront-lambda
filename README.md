Cloudfront Lambda
================

This is a simple ruby lambda function to parse CloudFront logs into JSON

Environment Variables
===================

`DEST_BUCKET` - The destination bucket your lambda will write processed logs into

`AWS_REGION` - Which region the source and destination log buckets are in

For more details about how to get this setup check out the [blog post](https://chaossearch.io/blog/reduce-complexity-and-quickly-search-amazon-cloudfront-logs-in-amazon-s3/)
