terraform {
  required_providers {
    flaky = {
      source = "terraform.io/test/flaky"
    }
  }
}

# Used by the fault injection / checkpoint recovery tests in
# context_apply_fault_test.go. The "name" attribute lets the mock provider
# identify which resource instance a given ApplyResourceChange call belongs
# to, so submissions can be counted across simulated restarts.
#
# The dependency chain makes the set of completed resources deterministic
# when a fault is injected while "c" is being applied: a and b can complete
# (and be checkpointed) first, c is deliberately held in flight by the test,
# and d cannot start until c has finished.
resource "flaky_node" "a" {
  name = "a"
}

resource "flaky_node" "b" {
  name = "b"
}

resource "flaky_node" "c" {
  name     = "c"
  depends_on = [flaky_node.a, flaky_node.b]
}

resource "flaky_node" "d" {
  name     = "d"
  depends_on = [flaky_node.c]
}
