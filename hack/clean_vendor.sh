find vendor -type f -not -name "*.go" -not -name "LICENSE" -not -name "*.s" -not -name "PATENTS" | xargs -I {} rm {}
# delete all test files
find vendor -type f -name "*_generated.go" | xargs -I {} rm {}
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
