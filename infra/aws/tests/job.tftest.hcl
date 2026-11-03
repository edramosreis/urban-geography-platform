# All provider data is synthetic; every scenario is a plan without AWS traffic.
mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/service-role/urban-geo-test-glue-writer" }
  }
}

variables {
  aws_region            = "eu-north-1"
  aws_account_id        = "123456789012"
  deployment_name       = "urban-geo-test"
  glue_job_release_json = file("../../tests/fixtures/glue_job_release.json")
  glue_release = {
    release_id      = jsondecode(file("../../tests/fixtures/glue_job_release.json")).release_id
    manifest_sha256 = filesha256("../../tests/fixtures/glue_job_release.json")
    summary_sha256  = jsondecode(file("../../tests/fixtures/glue_job_release.json")).identity.summary_sha256
    object_keys     = concat(values(jsondecode(file("../../tests/fixtures/glue_job_release.json")).keys), [jsondecode(file("../../tests/fixtures/glue_job_release.json")).completion_key])
  }
}

run "frozen_job_and_proposed_compute" {
  command = plan
  assert {
    condition = (
      aws_glue_job.pilot[0].name == "urban-geo-test-pilot-writer" &&
      aws_glue_job.pilot[0].role_arn == "arn:aws:iam::123456789012:role/service-role/urban-geo-test-glue-writer" &&
      aws_glue_job.pilot[0].glue_version == "5.1" && aws_glue_job.pilot[0].job_mode == "SCRIPT" &&
      aws_glue_job.pilot[0].execution_class == "STANDARD" &&
      aws_glue_job.pilot[0].worker_type == "G.1X" && aws_glue_job.pilot[0].number_of_workers == 2 &&
      aws_glue_job.pilot[0].timeout == 10 && aws_glue_job.pilot[0].max_retries == 0 &&
      !aws_glue_job.pilot[0].job_run_queuing_enabled &&
      one(aws_glue_job.pilot[0].execution_property).max_concurrent_runs == 1 &&
      one(aws_glue_job.pilot[0].command).name == "glueetl" &&
      one(aws_glue_job.pilot[0].command).python_version == "3" &&
      one(aws_glue_job.pilot[0].command).script_location == jsondecode(var.glue_job_release_json).job.script_location &&
      aws_glue_job.pilot[0].tags.ReleaseManifestSHA == var.glue_release.manifest_sha256
    )
    error_message = "The optional job must bind the reviewed release/role and explicit proposed compute settings."
  }
}

run "release_and_operational_arguments_cannot_be_overridden" {
  command = plan
  assert {
    condition = (
      length(aws_glue_job.pilot[0].default_arguments) == 0 &&
      aws_glue_job.pilot[0].non_overridable_arguments == tomap(merge(jsondecode(var.glue_job_release_json).job.arguments, {
        "--scriptLocation"          = jsondecode(var.glue_job_release_json).job.script_location
        "--job-language"            = "python"
        "--job-bookmark-option"     = "job-bookmark-disable"
        "--enable-auto-scaling"     = "false"
        "--custom-logGroup-prefix"  = "/urban-geography/urban-geo-test/glue"
        "--custom-logStream-prefix" = "urban-geo-test-pilot"
        "--TempDir"                 = "s3://urban-geo-test-lake-123456789012-eu-north-1/scratch/glue/${var.glue_release.release_id}/"
      }))
    )
    error_message = "All frozen software/input arguments, script override and operational settings must be non-overridable."
  }
}

run "scratch_is_separate_and_release_scoped" {
  command = plan
  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement) == 9 &&
      jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[7].Resource == "arn:aws:s3:::urban-geo-test-lake-123456789012-eu-north-1/scratch/glue/${var.glue_release.release_id}/*" &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[7].Action) == toset(["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]) &&
      toset(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement[8].Condition.StringLike["s3:prefix"]) == toset([
        "scratch/glue/${var.glue_release.release_id}", "scratch/glue/${var.glue_release.release_id}/*"
      ]) &&
      output.glue_job_scratch_uri == "s3://urban-geo-test-lake-123456789012-eu-north-1/scratch/glue/${var.glue_release.release_id}/"
    )
    error_message = "Temporary files must have separate release-scoped storage and permissions."
  }
}

run "deployment_policy_has_exact_job_and_passrole" {
  command = plan
  assert {
    condition = (
      toset(jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement[0].Action) == toset([
        "glue:CreateJob", "glue:GetJob", "glue:UpdateJob", "glue:DeleteJob",
        "glue:GetTags", "glue:TagResource", "glue:UntagResource",
        "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"
      ]) &&
      jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement[0].Resource == "arn:aws:glue:eu-north-1:123456789012:job/urban-geo-test-pilot-writer" &&
      jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement[1].Action == "iam:PassRole" &&
      jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement[1].Resource == "arn:aws:iam::123456789012:role/service-role/urban-geo-test-glue-writer" &&
      jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement[1].Condition.StringEquals["iam:PassedToService"] == "glue.amazonaws.com" &&
      length(jsondecode(aws_iam_policy.glue_job_deployment[0].policy).Statement) == 2
    )
    error_message = "Deployment may configure/inspect/stop only this job and pass only its role; no StartJobRun grant."
  }
}

run "iam_only_has_no_job_or_scratch_grant" {
  command = plan
  variables { glue_job_release_json = null }
  assert {
    condition = (
      length(aws_glue_job.pilot) == 0 && length(aws_iam_policy.glue_job_deployment) == 0 &&
      length(jsondecode(aws_iam_role_policy.glue_writer[0].policy).Statement) == 7 &&
      output.glue_job_name == null && output.glue_job_scratch_uri == null
    )
    error_message = "The existing IAM-only export must remain job-free without new scratch permissions."
  }
}

run "foundation_defaults_have_no_job" {
  command = plan
  variables {
    glue_job_release_json = null
    glue_release          = null
  }
  assert {
    condition     = length(aws_glue_job.pilot) == 0 && length(aws_iam_policy.glue_job_deployment) == 0
    error_message = "The original foundation must remain unchanged when optional inputs are omitted."
  }
}

run "job_requires_iam_descriptor" {
  command = plan
  variables { glue_release = null }
  expect_failures = [var.glue_job_release_json]
}

run "reject_manifest_pin_mismatch" {
  command = plan
  variables { glue_release = merge(var.glue_release, { manifest_sha256 = "0000000000000000000000000000000000000000000000000000000000000000" }) }
  expect_failures = [var.glue_job_release_json]
}

run "reject_unparseable_manifest" {
  command = plan
  variables { glue_job_release_json = "{invalid json" }
  expect_failures = [var.glue_job_release_json]
}

run "reject_byte_drift" {
  command = plan
  variables { glue_job_release_json = "${var.glue_job_release_json}\n" }
  expect_failures = [var.glue_job_release_json]
}

run "reject_wrong_target" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { identity = merge(jsondecode(var.glue_job_release_json).identity, { target = { deployment_name = "urban-geo-test", account_id = "999999999999", region = "eu-north-1" } }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { identity = merge(jsondecode(var.glue_job_release_json).identity, { target = { deployment_name = "urban-geo-test", account_id = "999999999999", region = "eu-north-1" } }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_wrong_script" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { script_location = "s3://unrelated/runtime/unapproved.py" }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { script_location = "s3://unrelated/runtime/unapproved.py" }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_unlisted_inventory" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { keys = merge(jsondecode(var.glue_job_release_json).keys, { unexpected = "inputs/unapproved/part.parquet" }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { keys = merge(jsondecode(var.glue_job_release_json).keys, { unexpected = "inputs/unapproved/part.parquet" }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_wrong_input_pin" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--summary-sha256" = "0000000000000000000000000000000000000000000000000000000000000000" }) }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--summary-sha256" = "0000000000000000000000000000000000000000000000000000000000000000" }) }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_unexpected_startup_key" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--extra-py-files" = "s3://unrelated/code.zip" }) }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--extra-py-files" = "s3://unrelated/code.zip" }) }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_unsupported_runtime" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { glue_version = "6.0" }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { glue_version = "6.0" }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}

run "reject_oversized_arguments" {
  command = plan
  variables {
    glue_job_release_json = jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--conf" = join("", [for block in range(300) : join("", [for char in range(1000) : "x"])]) }) }) }))
    glue_release          = merge(var.glue_release, { manifest_sha256 = sha256(jsonencode(merge(jsondecode(var.glue_job_release_json), { job = merge(jsondecode(var.glue_job_release_json).job, { arguments = merge(jsondecode(var.glue_job_release_json).job.arguments, { "--conf" = join("", [for block in range(300) : join("", [for char in range(1000) : "x"])]) }) }) }))) })
  }
  expect_failures = [aws_glue_job.pilot[0]]
}
