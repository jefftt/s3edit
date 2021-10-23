class S3edit < Formula
  version '0.0.1'
  desc "Make bulk S3 edits"
  homepage "https://github.com/jefftt/s3edit"

  if OS.mac?
      url "https://github.com/jefftt/s3edit/releases/download/#{version}/s3edit-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "7f73d029053a77e7d60564f03dd9e40fef5b210a38282249d4ebca552de8e847"
  end
end

