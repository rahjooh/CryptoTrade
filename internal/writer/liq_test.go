package writer

import "testing"

func TestNormalizeBucketName(t *testing.T) {
	bucket, err := normalizeBucketName(" my-bucket ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucket != "my-bucket" {
		t.Fatalf("expected trimmed bucket 'my-bucket', got %q", bucket)
	}
}

func TestNormalizeBucketNameRequiresValue(t *testing.T) {
	if _, err := normalizeBucketName("   \t  "); err == nil {
		t.Fatal("expected error for empty bucket")
	}
}
