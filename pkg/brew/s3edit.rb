class S3edit < Formula
  version '0.0.1'
  desc "Make bulk S3 edits"
  homepage "https://github.com/jefftt/s3edit"

  if OS.mac?
      url "https://github.com/jefftt/s3edit/releases/download/#{version}/s3edit-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "b294ac1bfb96684aaac10dd678666369345741108b452367a10d8c7fb4e7c47f"
  end

  def install
    bin.install "s3edit"
  end
end

