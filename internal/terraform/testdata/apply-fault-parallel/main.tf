terraform {
  required_providers {
    flaky = {
      source = "terraform.io/test/flaky"
    }
  }
}

# Used by the concurrent-failure test in context_apply_fault_test.go. These
# resources are independent so the apply walk visits them concurrently; the
# test holds "c" inside ApplyResourceChange until every other resource has
# been applied and checkpointed, then makes "c" fail.
resource "flaky_node" "a" {
  name = "a"
}

resource "flaky_node" "b" {
  name = "b"
}

resource "flaky_node" "c" {
  name = "c"
}

resource "flaky_node" "d" {
  name = "d"
}

resource "flaky_node" "e" {
  name = "e"
}

resource "flaky_node" "f" {
  name = "f"
}
