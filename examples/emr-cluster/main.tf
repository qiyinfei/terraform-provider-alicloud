terraform {
  required_providers {
    alicloud = {
      source  = "local.terraform.com/user/alicloud"
      version = "1.0.0"
    }
  }
}

provider "alicloud" {
  region = "cn-hangzhou"
}

resource "alicloud_emr_cluster_new" "default" {
  payment_type = "PayAsYouGo"
  cluster_type = "DATALAKE"
  release_version = "EMR-5.10.0"
  cluster_name = "terraform-emr"
  deploy_mode = "NORMAL"
  security_mode = "NORMAL"

  applications = ["HADOOP-COMMON", "HDFS", "YARN", "HIVE", "SPARK3", "TEZ"]

  application_configs {
    application_name = "HIVE"
    config_file_name = "hivemetastore-site.xml"
    config_item_key = "hive.metastore.type"
    config_item_value = "DLF"
    config_scope = "CLUSTER"
  }
  application_configs {
    application_name = "SPARK3"
    config_file_name = "hive-site.xml"
    config_item_key = "hive.metastore.type"
    config_item_value = "DLF"
    config_scope = "CLUSTER"
  }
  application_configs {
    application_name = "HIVE"
    config_file_name = "hivemetastore-site.xml"
    config_item_key = "dlf.catalog.id"
    config_item_value = "1509789347011222"
    config_scope = "CLUSTER"
  }
  application_configs {
    application_name = "SPARK3"
    config_file_name = "hive-site.xml"
    config_item_key = "dlf.catalog.id"
    config_item_value = "1509789347011222"
    config_scope = "CLUSTER"
  }

  node_attributes {
    ram_role = "AliyunECSInstanceForEMRRole"
    security_group_id = "sg-bp17hpmgz96tvnsdy6so"
    vpc_id = "vpc-bp1oycqbv7qinoxdmxrld"
    zone_id = "cn-hangzhou-i"
    key_pair_name = "emr_regression_key"
  }

  tags = {
    created = "tf"
  }

  node_groups {
    node_group_type = "MASTER"
    node_group_name = "emr-master"
    payment_type = "PayAsYouGo"
    auto_pay_order = "false"
    v_switch_ids = ["vsw-bp12hbwofguezvchcmsre"]
    with_public_ip = false
    instance_types = ["ecs.g7.xlarge"]
    node_count = 1
    system_disk {
      category = "cloud_essd"
      size = 80
      count = 1
    }
    data_disks {
      category = "cloud_essd"
      size = 80
      count = 3
    }
    deployment_set_strategy = "NONE"
  }
  node_groups {
    node_group_type = "CORE"
    node_group_name = "emr-core"
    payment_type = "PayAsYouGo"
    auto_pay_order = "false"
    v_switch_ids = ["vsw-bp12hbwofguezvchcmsre"]
    with_public_ip = false
    instance_types = ["ecs.g7.xlarge"]
    node_count = 3
    system_disk {
      category = "cloud_essd"
      size = 80
      count = 1
    }
    data_disks {
      category = "cloud_essd"
      size = 80
      count = 3
    }
    deployment_set_strategy = "NONE"
  }

  resource_group_id = "rg-acfmzabjyopnvfq"
}