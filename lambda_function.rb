require 'zlib'
require 'time'
require 'json'
require 'aws-sdk-s3'

def lambda_handler(event:, context:)
  event = event['Records'].first
  filename = event['s3']['object']['key']
  source_bucket = event['s3']['bucket']['name']

  destination_bucket = ENV['DEST_BUCKET']
  aws_region = ENV['AWS_REGION']
  filedate = Date.parse(filename.split('.')[1]).to_s

  s3 = Aws::S3::Resource.new(region: aws_region)

  source_file = s3.bucket(source_bucket).object(filename)

  data = Zlib::GzipReader.new(source_file.get.body).read.split("\n")

  logfile = Array.new

  def gzip(data)
    sio = StringIO.new
    gz = Zlib::GzipWriter.new(sio)
    gz.write(data)
    gz.close
    sio.string
  end

  logline_schema = [
    'date', # will be merged into new timestamp field
    'time', # will be merged into new timestamp field
    'edge_location',
    'sc_bytes',
    'c_ip',
    'cs_method',
    'cs_host',
    'cs_uri_stem',
    'sc_status',
    'cs_referer',
    'cs_user_agent',
    'cs_uri_query',
    'cs_cookie',
    'edge_result_type',
    'edge_request_id',
    'host_header',
    'cs_protocol',
    'cs_bytes',
    'time_taken',
    'forwarded_for',
    'ssl_protocol',
    'ssl_cipher',
    'edge_response_result_type',
    'cs_protocol_version',
    'fle_status',
    'fle_encrypted_fields'
  ]


  data.each do |line|
    logline = Hash.new
    unless line.start_with?("#")
      line.split("\t").each_with_index do |line_value, idx|
        logline[logline_schema[idx]] = line_value
      end
      logline['timestamp'] = Time.parse("#{logline['date']} #{logline['time']} UTC").iso8601
      logfile << logline.to_json
    end
  end

  processed_filename = "#{File.basename(filename, '.gz')}-processed.json.gz"
  obj = s3.bucket(destination_bucket).object([filedate,processed_filename].join('/'))
  begin
    response = obj.put(body: gzip(logfile.join("\n")))
  rescue Aws::S3::Errors::ServiceError => e
    puts e.message
  end

  puts "Sucessfully wrote #{processed_filename} with etag #{response.etag}"
end
