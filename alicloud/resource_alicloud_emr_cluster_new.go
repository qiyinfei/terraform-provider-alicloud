package alicloud

import (
	"fmt"
	"strings"
	"time"

	"github.com/PaesslerAG/jsonpath"
	util "github.com/alibabacloud-go/tea-utils/service"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"

	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"

	"github.com/aliyun/terraform-provider-alicloud/alicloud/connectivity"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceAlicloudEmrClusterNew() *schema.Resource {
	return &schema.Resource{
		Create: resourceAlicloudEmrClusterCreateNew,
		Read:   resourceAlicloudEmrClusterReadNew,
		Update: resourceAlicloudEmrClusterUpdateNew,
		Delete: resourceAlicloudEmrClusterDeleteNew,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(20 * time.Minute),
			Delete: schema.DefaultTimeout(10 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"resource_group_id": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"payment_type": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				Default:      "PayAsYouGo",
				ValidateFunc: validation.StringInSlice([]string{"PayAsYouGo", "Subscription"}, false),
			},
			"subscription_config": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"payment_duration_unit": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"Month", "Year"}, false),
						},
						"payment_duration": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntInSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 24, 36, 48, 60}),
						},
						"auto_pay_order": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"auto_renew": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"auto_renew_duration_unit": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice([]string{"Month", "Year"}, false),
						},
						"auto_renew_duration": {
							Type:         schema.TypeInt,
							Optional:     true,
							ValidateFunc: validation.IntInSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 24, 36, 48, 60}),
						},
					},
				},
				MaxItems: 1,
			},
			"cluster_type": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice([]string{"DATALAKE", "OLAP", "DATAFLOW", "DATASERVING", "CUSTOM"}, false),
			},
			"release_version": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"cluster_name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"deploy_mode": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringInSlice([]string{"NORMAL", "HA"}, false),
			},
			"security_mode": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringInSlice([]string{"NORMAL", "KERBEROS"}, false),
			},
			"applications": {
				Type:     schema.TypeList,
				Required: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"application_configs": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"application_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"config_file_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"config_item_key": {
							Type:     schema.TypeString,
							Required: true,
						},
						"config_item_value": {
							Type:     schema.TypeString,
							Required: true,
						},
						"config_scope": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringInSlice([]string{"CLUSTER", "NODE_GROUP"}, false),
						},
						"config_description": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"node_group_name": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"node_group_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
			"node_attributes": {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"vpc_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"zone_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"security_group_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"ram_role": {
							Type:     schema.TypeString,
							Required: true,
						},
						"key_pair_name": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
				MaxItems: 1,
			},
			"node_groups": {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"node_group_type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"MASTER", "CORE", "TASK"}, false),
						},
						"node_group_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"payment_type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"PayAsYouGo", "Subscription"}, false),
						},
						"auto_pay_order": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"subscription_config": {
							Type:     schema.TypeSet,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"payment_duration_unit": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice([]string{"Month", "Year"}, false),
									},
									"payment_duration": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntInSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 24, 36, 48, 60}),
									},
									"auto_renew": {
										Type:     schema.TypeBool,
										Optional: true,
									},
									"auto_renew_duration_unit": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringInSlice([]string{"Month", "Year"}, false),
									},
									"auto_renew_duration": {
										Type:         schema.TypeInt,
										Optional:     true,
										ValidateFunc: validation.IntInSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 24, 36, 48, 60}),
									},
								},
							},
							MaxItems: 1,
						},
						"spot_strategy": {
							Type:         schema.TypeString,
							Optional:     true,
							Default:      "NoSpot",
							ValidateFunc: validation.StringInSlice([]string{"NoSpot", "SpotWithPriceLimit", "SpotAsPriceGo"}, false),
						},
						"spot_bid_prices": {
							Type:     schema.TypeSet,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"instance_type": {
										Type:     schema.TypeString,
										Required: true,
									},
									"bid_price": {
										Type:     schema.TypeInt,
										Required: true,
									},
								},
							},
						},
						"v_switch_ids": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"with_public_ip": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"additional_security_group_ids": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"instance_types": {
							Type:     schema.TypeList,
							Required: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"node_count": {
							Type:     schema.TypeInt,
							Required: true,
						},
						"system_disk": {
							Type:     schema.TypeSet,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"category": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice([]string{"cloud_essd", "cloud_efficiency"}, false),
									},
									"size": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"performance_level": {
										Type:         schema.TypeString,
										Optional:     true,
										Default:      "PL1",
										ValidateFunc: validation.StringInSlice([]string{"PL0", "PL1", "PL2", "PL3"}, false),
									},
									"count": {
										Type:     schema.TypeInt,
										Optional: true,
										Default:  1,
									},
								},
							},
							MaxItems: 1,
						},
						"data_disks": {
							Type:     schema.TypeSet,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"category": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice([]string{"cloud_efficiency", "cloud_ssd", "cloud_essd", "cloud", "local_hdd_pro"}, false),
									},
									"size": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"performance_level": {
										Type:         schema.TypeString,
										Optional:     true,
										Default:      "PL1",
										ValidateFunc: validation.StringInSlice([]string{"PL0", "PL1", "PL2", "PL3"}, false),
									},
									"count": {
										Type:     schema.TypeInt,
										Required: true,
									},
								},
							},
						},
						"graceful_shutdown": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"spot_instance_remedy": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"node_resize_strategy": {
							Type:         schema.TypeString,
							Optional:     true,
							Default:      "PRIORITY",
							ValidateFunc: validation.StringInSlice([]string{"COST_OPTIMIZED", "PRIORITY"}, false),
						},
						"cost_optimized_config": {
							Type:     schema.TypeSet,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"on_demand_base_capacity": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"on_demand_percentage_above_base_capacity": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"spot_instance_pools": {
										Type:     schema.TypeInt,
										Required: true,
									},
								},
							},
							MaxItems: 1,
						},
						"deployment_set_strategy": {
							Type:         schema.TypeString,
							Optional:     true,
							Default:      "NONE",
							ValidateFunc: validation.StringInSlice([]string{"NONE", "CLUSTER", "NODE_GROUP"}, false),
						},
					},
				},
			},
			"bootstrap_scripts": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"script_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"script_path": {
							Type:     schema.TypeString,
							Required: true,
						},
						"script_args": {
							Type:     schema.TypeString,
							Required: true,
						},
						"priority": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"execution_moment": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"BEFORE_INSTALL", "AFTER_STARTED"}, false),
						},
						"execution_fail_strategy": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"FAILED_CONTINUE", "FAILED_BLOCK"}, false),
						},
						"node_selector": {
							Type:     schema.TypeSet,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"node_select_type": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice([]string{"CLUSTER", "NODE_GROUP", "NODE"}, false),
									},
									"node_names": {
										Type:     schema.TypeList,
										Optional: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
									"node_group_id": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"node_group_types": {
										Type:     schema.TypeList,
										Optional: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
									"node_group_name": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
							MaxItems: 1,
						},
					},
				},
			},
			"tags": tagsSchemaComputed(),
			"client_token": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceAlicloudEmrClusterCreateNew(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*connectivity.AliyunClient)
	var response map[string]interface{}
	action := "CreateCluster"
	conn, err := client.NewEmrClient()
	if err != nil {
		return WrapError(err)
	}
	createClusterRequest := map[string]interface{}{
		"RegionId": client.RegionId,
	}
	if v, ok := d.GetOk("resource_group_id"); ok {
		createClusterRequest["ResourceGroupId"] = v
	}

	if v, ok := d.GetOk("payment_type"); ok {
		createClusterRequest["PaymentType"] = v
	}

	if v, ok := d.GetOk("subscription_config"); ok {
		subscriptionConfig := v.(*schema.Set).List()
		if len(subscriptionConfig) == 1 {
			subscriptionConfigSource := subscriptionConfig[0].(map[string]interface{})
			subscriptionConfigMap := map[string]interface{}{
				"PaymentDurationUnit":   subscriptionConfigSource["payment_duration_unit"],
				"PaymentDuration":       subscriptionConfigSource["payment_duration"],
				"AutoPayOrder":          subscriptionConfigSource["auto_pay_order"],
				"AutoRenew":             subscriptionConfigSource["auto_renew"],
				"AutoRenewDurationUnit": subscriptionConfigSource["auto_renew_duration_unit"],
				"AutoRenewDuration":     subscriptionConfigSource["auto_renew_duration"],
			}
			createClusterRequest["SubscriptionConfig"] = subscriptionConfigMap
		}
	}

	if v, ok := d.GetOk("cluster_type"); ok {
		createClusterRequest["ClusterType"] = v
	}

	if v, ok := d.GetOk("release_version"); ok {
		createClusterRequest["ReleaseVersion"] = v
	}

	if v, ok := d.GetOk("cluster_name"); ok {
		createClusterRequest["ClusterName"] = v
	}

	if v, ok := d.GetOk("deploy_mode"); ok {
		createClusterRequest["DeployMode"] = v
	}

	if v, ok := d.GetOk("security_mode"); ok {
		createClusterRequest["SecurityMode"] = v
	}

	applications := make([]map[string]interface{}, 0)
	if apps, ok := d.GetOk("applications"); ok {
		for _, application := range apps.([]interface{}) {
			applications = append(applications, map[string]interface{}{"ApplicationName": application.(string)})
		}
	}
	createClusterRequest["Applications"] = applications

	applicationConfigs := make([]map[string]interface{}, 0)
	if appConfigs, ok := d.GetOk("application_configs"); ok {
		for _, appConfig := range appConfigs.(*schema.Set).List() {
			applicationConfig := map[string]interface{}{}
			kv := appConfig.(map[string]interface{})
			if v, ok := kv["application_name"]; ok {
				applicationConfig["ApplicationName"] = v
			}
			if v, ok := kv["config_file_name"]; ok {
				applicationConfig["ConfigFileName"] = v
			}
			if v, ok := kv["config_item_key"]; ok {
				applicationConfig["ConfigItemKey"] = v
			}
			if v, ok := kv["config_item_value"]; ok {
				applicationConfig["ConfigItemValue"] = v
			}
			if v, ok := kv["config_scope"]; ok {
				applicationConfig["ConfigScope"] = v
			}
			if v, ok := kv["node_group_name"]; ok {
				applicationConfig["NodeGroupName"] = v
			}
			if v, ok := kv["node_group_id"]; ok {
				applicationConfig["NodeGroupId"] = v
			}
			applicationConfigs = append(applicationConfigs, applicationConfig)
		}
	}
	createClusterRequest["ApplicationConfigs"] = applicationConfigs

	if v, ok := d.GetOk("node_attributes"); ok {
		nodeAttributes := v.(*schema.Set).List()
		if len(nodeAttributes) == 1 {
			nodeAttributesSource := nodeAttributes[0].(map[string]interface{})
			nodeAttributesSourceMap := map[string]interface{}{
				"VpcId":           nodeAttributesSource["vpc_id"],
				"ZoneId":          nodeAttributesSource["zone_id"],
				"SecurityGroupId": nodeAttributesSource["security_group_id"],
				"RamRole":         nodeAttributesSource["ram_role"],
				"KeyPairName":     nodeAttributesSource["key_pair_name"],
			}
			createClusterRequest["NodeAttributes"] = nodeAttributesSourceMap
		}
	}

	nodeGroups := make([]map[string]interface{}, 0)
	if nodeGroupsList, ok := d.GetOk("node_groups"); ok {
		for _, nodeGroupItem := range nodeGroupsList.(*schema.Set).List() {
			nodeGroup := map[string]interface{}{}
			kv := nodeGroupItem.(map[string]interface{})
			if v, ok := kv["node_group_type"]; ok {
				nodeGroup["NodeGroupType"] = v
			}
			if v, ok := kv["node_group_name"]; ok {
				nodeGroup["NodeGroupName"] = v
			}
			if v, ok := kv["payment_type"]; ok {
				nodeGroup["PaymentType"] = v
			}
			if v, ok := kv["subscription_config"]; ok {
				subscriptionConfigs := v.(*schema.Set).List()
				if len(subscriptionConfigs) == 1 {
					subscriptionConfig := map[string]interface{}{}
					subscriptionConfigMap := subscriptionConfigs[0].(map[string]interface{})
					if value, exists := subscriptionConfigMap["payment_duration_unit"]; exists {
						subscriptionConfig["PaymentDurationUnit"] = value
					}
					if value, exists := subscriptionConfigMap["payment_duration"]; exists {
						subscriptionConfig["PaymentDuration"] = value
					}
					if value, exists := subscriptionConfigMap["auto_renew"]; exists {
						subscriptionConfig["AutoRenew"] = value
					}
					if value, exists := subscriptionConfigMap["auto_renew_duration_unit"]; exists {
						subscriptionConfig["AutoRenewDurationUnit"] = value
					}
					if value, exists := subscriptionConfigMap["auto_renew_duration"]; exists {
						subscriptionConfig["AutoRenewDuration"] = value
					}
					nodeGroup["SubscriptionConfig"] = subscriptionConfig
				}
			}
			if v, ok := kv["spot_strategy"]; ok {
				nodeGroup["SpotStrategy"] = v
			}
			if v, ok := kv["spot_bid_prices"]; ok {
				spotBidPriceList := v.(*schema.Set).List()
				if len(spotBidPriceList) > 0 {
					spotBidPrices := make([]map[string]interface{}, 0)
					for _, spotBidPriceSource := range spotBidPriceList {
						spotBidPrice := map[string]interface{}{}
						spotBidPriceMap := spotBidPriceSource.(map[string]interface{})
						if value, exists := spotBidPriceMap["instance_type"]; exists {
							spotBidPrice["InstanceType"] = value
						}
						if value, exists := spotBidPriceMap["bid_price"]; exists {
							spotBidPrice["BidPrice"] = value
						}
						spotBidPrices = append(spotBidPrices, spotBidPrice)
					}
					nodeGroup["SpotBidPrices"] = spotBidPrices
				}
			}
			if v, ok := kv["v_switch_ids"]; ok {
				var vSwitchIds []string
				for _, vSwitchId := range v.([]interface{}) {
					vSwitchIds = append(vSwitchIds, vSwitchId.(string))
				}
				nodeGroup["VSwitchIds"] = vSwitchIds
			}
			if v, ok := kv["with_public_ip"]; ok {
				nodeGroup["WithPublicIp"] = v
			}
			if v, ok := kv["additional_security_group_ids"]; ok {
				var additionalSecurityGroupIds []string
				for _, additionalSecurityGroupId := range v.([]interface{}) {
					additionalSecurityGroupIds = append(additionalSecurityGroupIds, additionalSecurityGroupId.(string))
				}
				nodeGroup["AdditionalSecurityGroupIds"] = additionalSecurityGroupIds
			}
			if v, ok := kv["instance_types"]; ok {
				var instanceTypes []string
				for _, instanceType := range v.([]interface{}) {
					instanceTypes = append(instanceTypes, instanceType.(string))
				}
				nodeGroup["InstanceTypes"] = instanceTypes
			}
			if v, ok := kv["node_count"]; ok {
				nodeGroup["NodeCount"] = v
			}
			if v, ok := kv["system_disk"]; ok {
				systemDisks := v.(*schema.Set).List()
				if len(systemDisks) == 1 {
					systemDisk := map[string]interface{}{}
					systemDiskMap := systemDisks[0].(map[string]interface{})
					if value, exists := systemDiskMap["category"]; exists {
						systemDisk["Category"] = value
					}
					if value, exists := systemDiskMap["size"]; exists {
						systemDisk["Size"] = value
					}
					if value, exists := systemDiskMap["performance_level"]; exists {
						systemDisk["PerformanceLevel"] = value
					}
					if value, exists := systemDiskMap["count"]; exists {
						systemDisk["Count"] = value
					}
					nodeGroup["SystemDisk"] = systemDisk
				}
			}
			if v, ok := kv["data_disks"]; ok {
				dataDiskList := v.(*schema.Set).List()
				if len(dataDiskList) > 0 {
					dataDisks := make([]map[string]interface{}, 0)
					for _, dataDiskSource := range dataDiskList {
						dataDisk := map[string]interface{}{}
						dataDiskMap := dataDiskSource.(map[string]interface{})
						if value, exists := dataDiskMap["category"]; exists {
							dataDisk["Category"] = value
						}
						if value, exists := dataDiskMap["size"]; exists {
							dataDisk["Size"] = value
						}
						if value, exists := dataDiskMap["performance_level"]; exists {
							dataDisk["PerformanceLevel"] = value
						}
						if value, exists := dataDiskMap["count"]; exists {
							dataDisk["Count"] = value
						}
						dataDisks = append(dataDisks, dataDisk)
					}
					nodeGroup["DataDisks"] = dataDisks
				}
			}
			if v, ok := kv["graceful_shutdown"]; ok {
				nodeGroup["GracefulShutdown"] = v
			}
			if v, ok := kv["spot_instance_remedy"]; ok {
				nodeGroup["SpotInstanceRemedy"] = v
			}
			if v, ok := kv["node_resize_strategy"]; ok {
				nodeGroup["NodeResizeStrategy"] = v
			}
			if v, ok := kv["cost_optimized_config"]; ok {
				costOptimizedConfigs := v.(*schema.Set).List()
				if len(costOptimizedConfigs) == 1 {
					costOptimizedConfig := map[string]interface{}{}
					costOptimizedConfigMap := costOptimizedConfigs[0].(map[string]interface{})
					if value, exists := costOptimizedConfigMap["on_demand_base_capacity"]; exists {
						costOptimizedConfig["OnDemandBaseCapacity"] = value
					}
					if value, exists := costOptimizedConfigMap["on_demand_percentage_above_base_capacity"]; exists {
						costOptimizedConfig["OnDemandPercentageAboveBaseCapacity"] = value
					}
					if value, exists := costOptimizedConfigMap["spot_instance_pools"]; exists {
						costOptimizedConfig["SpotInstancePools"] = value
					}
					nodeGroup["CostOptimizedConfig"] = costOptimizedConfig
				}
			}
			if v, ok := kv["deployment_set_strategy"]; ok {
				nodeGroup["DeploymentSetStrategy"] = v
			}
			nodeGroups = append(nodeGroups, nodeGroup)
		}
	}
	createClusterRequest["NodeGroups"] = nodeGroups

	if scripts, ok := d.GetOk("bootstrap_scripts"); ok {
		bootstrapScripts := make([]map[string]interface{}, 0)
		for _, script := range scripts.(*schema.Set).List() {
			kv := script.(map[string]interface{})
			bootstrapScript := map[string]interface{}{}
			if v, ok := kv["script_name"]; ok {
				bootstrapScript["ScriptName"] = v
			}
			if v, ok := kv["script_path"]; ok {
				bootstrapScript["ScriptPath"] = v
			}
			if v, ok := kv["script_args"]; ok {
				bootstrapScript["ScriptArgs"] = v
			}
			if v, ok := kv["priority"]; ok {
				bootstrapScript["Priority"] = v
			}
			if v, ok := kv["execution_moment"]; ok {
				bootstrapScript["ExecutionMoment"] = v
			}
			if v, ok := kv["execution_fail_strategy"]; ok {
				bootstrapScript["ExecutionFailStrategy"] = v
			}
			if v, ok := kv["node_selector"]; ok {
				nodeSelectors := v.(*schema.Set).List()
				if len(nodeSelectors) == 1 {
					nodeSelector := map[string]interface{}{}
					nodeSelectorMap := nodeSelectors[0].(map[string]interface{})
					if value, exists := nodeSelectorMap["node_select_type"]; exists {
						nodeSelector["NodeSelectType"] = value
					}
					if value, exists := nodeSelectorMap["node_names"]; exists {
						var nodeNames []string
						for _, nodeName := range value.([]interface{}) {
							nodeNames = append(nodeNames, nodeName.(string))
						}
						nodeSelector["NodeNames"] = nodeNames
					}
					if value, exists := nodeSelectorMap["node_group_id"]; exists {
						nodeSelector["NodeGroupId"] = value
					}
					if value, exists := nodeSelectorMap["node_group_types"]; exists {
						var nodeGroupTypes []string
						for _, nodeGroupType := range value.([]interface{}) {
							nodeGroupTypes = append(nodeGroupTypes, nodeGroupType.(string))
						}
						nodeSelector["NodeGroupTypes"] = nodeGroupTypes
					}
					if value, exists := nodeSelectorMap["node_group_name"]; exists {
						nodeSelector["NodeGroupName"] = value
					}
					bootstrapScript["NodeSelector"] = nodeSelector
				}
			}
			bootstrapScripts = append(bootstrapScripts, bootstrapScript)
		}
		createClusterRequest["BootstrapScripts"] = bootstrapScripts
	}

	if v, ok := d.GetOk("tags"); ok {
		tags := make([]map[string]interface{}, 0)
		for key, value := range v.(map[string]interface{}) {
			tags = append(tags, map[string]interface{}{
				"Key":   key,
				"Value": value,
			})
		}
		createClusterRequest["Tags"] = tags
	}

	if v, ok := d.GetOk("client_token"); ok {
		createClusterRequest["ClientToken"] = v
	}

	runtime := util.RuntimeOptions{}
	runtime.SetAutoretry(true)
	wait := incrementalWait(3*time.Second, 5*time.Second)
	err = resource.Retry(d.Timeout(schema.TimeoutCreate), func() *resource.RetryError {
		response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, createClusterRequest, &runtime)
		if err != nil {
			if NeedRetry(err) {
				wait()
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	addDebug(action, response, createClusterRequest)

	if err != nil {
		return WrapErrorf(err, DefaultErrorMsg, "alicloud_emr_cluster_new", action, AlibabaCloudSdkGoERROR)
	}

	d.SetId(fmt.Sprint(response["ClusterId"]))

	emrService := EmrService{client}
	stateConf := BuildStateConf([]string{"STARTING"}, []string{"RUNNING"}, d.Timeout(schema.TimeoutCreate),
		90*time.Second, emrService.EmrClusterNewStateRefreshFunc(d.Id(), []string{"START_FAILED", "TERMINATED_WITH_ERRORS", "TERMINATED"}))
	stateConf.PollInterval = 10 * time.Second
	if _, err := stateConf.WaitForState(); err != nil {
		return WrapErrorf(err, IdMsg, d.Id())
	}

	return resourceAlicloudEmrClusterReadNew(d, meta)
}

func resourceAlicloudEmrClusterReadNew(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*connectivity.AliyunClient)
	emrService := EmrService{client}

	object, err := emrService.GetEmrClusterNew(d.Id())

	if err != nil {
		if NotFoundError(err) {
			d.SetId("")
			return nil
		}
		return WrapError(err)
	}

	d.Set("cluster_name", object["ClusterName"])

	tags, err := emrService.ListTagResourcesNew(d.Id(), string(TagResourceCluster))

	if err != nil {
		return WrapError(err)
	}
	d.Set("tags", tagsToMap(tags))

	return nil
}

func resourceAlicloudEmrClusterUpdateNew(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*connectivity.AliyunClient)
	conn, err := client.NewEmrClient()
	if err != nil {
		return WrapError(err)
	}
	var response map[string]interface{}
	d.Partial(true)
	emrService := EmrService{client}
	if err := emrService.SetEmrClusterTagsNew(d); err != nil {
		return WrapError(err)
	}

	if d.HasChange("cluster_name") {
		action := "UpdateClusterAttribute"
		request := map[string]interface{}{
			"ClusterId":   d.Id(),
			"RegionId":    client.RegionId,
			"ClusterName": d.Get("cluster_name"),
		}
		wait := incrementalWait(3*time.Second, 3*time.Second)
		err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
			response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, request, &util.RuntimeOptions{})
			if err != nil {
				if NeedRetry(err) {
					wait()
					return resource.RetryableError(err)
				}
				return resource.NonRetryableError(err)
			}
			addDebug(action, response, request)
			return nil
		})
		if err != nil {
			return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
		}
		d.SetPartial("cluster_name")
	}

	if d.HasChange("node_groups") {
		_, newNodeGroupsList := d.GetChange("node_groups")

		listNodeGroupsRequest := map[string]interface{}{
			"ClusterId": d.Id(),
			"RegionId":  client.RegionId,
		}
		action := "ListNodeGroups"
		runtime := util.RuntimeOptions{}
		runtime.SetAutoretry(true)
		wait := incrementalWait(3*time.Second, 5*time.Second)
		err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
			response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, listNodeGroupsRequest, &runtime)
			if err != nil {
				if NeedRetry(err) {
					wait()
					return resource.RetryableError(err)
				}
				return resource.NonRetryableError(err)
			}
			return nil
		})
		addDebug(action, response, listNodeGroupsRequest)
		if err != nil {
			return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
		}
		resp, err := jsonpath.Get("$.NodeGroups", response)

		if err != nil {
			return WrapErrorf(err, FailedGetAttributeMsg, d.Id(), "$.NodeGroups", response)
		}

		oldNodeGroupMap := map[string]map[string]interface{}{}
		for _, nodeGroupItem := range resp.([]interface{}) {
			oldNodeGroup := nodeGroupItem.(map[string]interface{})
			oldNodeGroupMap[oldNodeGroup["NodeGroupName"].(string)] = oldNodeGroup
		}

		newNodeGroupMap := map[string]map[string]interface{}{}
		for _, nodeGroupItem := range newNodeGroupsList.(*schema.Set).List() {
			newNodeGroup := nodeGroupItem.(map[string]interface{})
			newNodeGroupMap[newNodeGroup["node_group_name"].(string)] = newNodeGroup
		}

		var increaseNodesGroups []map[string]interface{}
		var decreaseNodesGroups []map[string]interface{}

		for nodeGroupName, newNodeGroup := range newNodeGroupMap {
			if oldNodeGroup, ok := oldNodeGroupMap[nodeGroupName]; ok {
				newNodeCount := formatInt(newNodeGroup["node_count"])
				oldNodeCount := formatInt(oldNodeGroup["RunningNodeCount"])

				// increase nodes
				if oldNodeCount < newNodeCount {
					count := newNodeCount - oldNodeCount
					increaseNodesGroup := map[string]interface{}{}
					increaseNodesGroup["RegionId"] = client.RegionId
					increaseNodesGroup["ClusterId"] = d.Id()
					increaseNodesGroup["NodeGroupId"] = oldNodeGroup["NodeGroupId"]
					increaseNodesGroup["IncreaseNodeCount"] = count
					increaseNodesGroup["AutoPayOrder"] = newNodeGroup["auto_pay_order"]
					if "Subscription" == newNodeGroup["payment_type"].(string) {
						subscriptionConfig := newNodeGroup["subscription_config"].(*schema.Set).List()
						if len(subscriptionConfig) == 1 {
							configMap := subscriptionConfig[0].(map[string]interface{})
							increaseNodesGroup["PaymentDuration"] = configMap["payment_duration"]
							increaseNodesGroup["PaymentDurationUnit"] = configMap["payment_duration_unit"]
						}
					}
					increaseNodesGroups = append(increaseNodesGroups, increaseNodesGroup)
				} else if oldNodeCount > newNodeCount { // decrease nodes
					// EMR cluster can only decrease 'TASK' node group.
					if "TASK" != newNodeGroup["node_group_type"].(string) {
						return WrapError(Error("EMR cluster can only decrease the node group type of [TASK]."))
					}
					decreaseNodesGroup := map[string]interface{}{
						"ClusterId":         d.Id(),
						"RegionId":          client.RegionId,
						"DecreaseNodeCount": oldNodeCount - newNodeCount,
						"NodeGroupId":       oldNodeGroup["NodeGroupId"],
					}
					decreaseNodesGroups = append(decreaseNodesGroups, decreaseNodesGroup)
				}

				// increase node disk size, we can only support single disk type.
				currDataDisk := oldNodeGroup["DataDisks"].([]interface{})[0].(map[string]interface{})
				targetDataDisk := newNodeGroup["data_disks"].(*schema.Set).List()[0].(map[string]interface{})
				if formatInt(targetDataDisk["size"]) < formatInt(currDataDisk["Size"]) {
					return WrapError(Error("EMR cluster can only increase node disk, decrease node disk is not supported."))
				} else if formatInt(targetDataDisk["size"]) > formatInt(currDataDisk["Size"]) {
					if currDataDisk["Category"].(string) == "local_hdd_pro" {
						return WrapError(Error("EMR cluster can not support increase node disk with 'local_hdd_pro' disk type."))
					}
					action := "IncreaseNodesDiskSize"
					increaseNodeDiskSizeRequest := map[string]interface{}{
						"ClusterId":   d.Id(),
						"RegionId":    client.RegionId,
						"NodeGroupId": oldNodeGroup["NodeGroupId"],
						"DataDiskSizes": []map[string]interface{}{
							{
								"Category": currDataDisk["Category"].(string),
								"Size":     formatInt(targetDataDisk["size"]),
							},
						},
					}
					runtime := util.RuntimeOptions{}
					runtime.SetAutoretry(true)
					wait := incrementalWait(3*time.Second, 5*time.Second)
					err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
						response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, increaseNodeDiskSizeRequest, &runtime)
						if err != nil {
							if NeedRetry(err) {
								wait()
								return resource.RetryableError(err)
							}
							return resource.NonRetryableError(err)
						}
						return nil
					})
					addDebug(action, response, increaseNodeDiskSizeRequest)
					if err != nil {
						return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
					}
				}
			} else { // 'Task' NodeGroupType may not exist when create emr_cluster
				subscriptionConfig := map[string]interface{}{}
				if "Subscription" == newNodeGroup["payment_type"] {
					subscriptionMap := newNodeGroup["subscription_config"].(map[string]interface{})
					subscriptionConfig["PaymentDurationUnit"] = subscriptionMap["payment_duration_unit"]
					subscriptionConfig["PaymentDuration"] = subscriptionMap["payment_duration"]
					subscriptionConfig["AutoRenew"] = subscriptionMap["auto_renew"]
					subscriptionConfig["AutoRenewDurationUnit"] = subscriptionMap["auto_renew_duration_init"]
					subscriptionConfig["AutoRenewDuration"] = subscriptionMap["auto_renew_duration"]
				}
				var spotBidPrices []map[string]interface{}
				for _, v := range newNodeGroup["spot_bid_prices"].(*schema.Set).List() {
					sbpMap := v.(map[string]interface{})
					spotBidPrices = append(spotBidPrices, map[string]interface{}{
						"InstanceType": sbpMap["instance_type"],
						"BidPrice":     sbpMap["bid_price"],
					})
				}
				systemDiskMap := newNodeGroup["system_disk"].(*schema.Set).List()[0].(map[string]interface{})
				var dataDisks []map[string]interface{}
				for _, v := range newNodeGroup["data_disks"].(*schema.Set).List() {
					dataDiskMap := v.(map[string]interface{})
					dataDisks = append(dataDisks, map[string]interface{}{
						"Category":         dataDiskMap["category"],
						"Size":             dataDiskMap["size"],
						"PerformanceLevel": dataDiskMap["performance_level"],
						"Count":            dataDiskMap["count"],
					})
				}
				createNodeGroupRequest := map[string]interface{}{
					"ClusterId": d.Id(),
					"RegionId":  client.RegionId,
					"NodeGroup": map[string]interface{}{
						"NodeGroupType":              newNodeGroup["node_group_type"],
						"NodeGroupName":              nodeGroupName,
						"PaymentType":                newNodeGroup["payment_type"],
						"SubscriptionConfig":         subscriptionConfig,
						"SpotStrategy":               newNodeGroup["spot_strategy"],
						"SpotBidPrices":              spotBidPrices,
						"VSwitchIds":                 newNodeGroup["v_switch_ids"],
						"WithPublicIp":               newNodeGroup["with_public_ip"],
						"AdditionalSecurityGroupIds": newNodeGroup["additional_security_group_ids"],
						"InstanceTypes":              newNodeGroup["instance_types"],
						"NodeCount":                  newNodeGroup["node_count"],
						"SystemDisk": map[string]interface{}{
							"Category":         systemDiskMap["category"],
							"Size":             systemDiskMap["size"],
							"PerformanceLevel": systemDiskMap["performance_level"],
							"Count":            systemDiskMap["count"],
						},
						"DataDisks":             dataDisks,
						"GracefulShutdown":      newNodeGroup["graceful_shutdown"],
						"SpotInstanceRemedy":    newNodeGroup["spot_instance_remedy"],
						"NodeResizeStrategy":    newNodeGroup["node_resize_strategy"],
						"DeploymentSetStrategy": newNodeGroup["deployment_set_strategy"],
					},
				}
				costOptimizedConfigList := newNodeGroup["cost_optimized_config"].(*schema.Set).List()
				if len(costOptimizedConfigList) > 0 {
					costOptimizedConfig := costOptimizedConfigList[0].(map[string]interface{})
					createNodeGroupRequest["CostOptimizedConfig"] = map[string]interface{}{
						"OnDemandBaseCapacity":                costOptimizedConfig["on_demand_base_capacity"],
						"OnDemandPercentageAboveBaseCapacity": costOptimizedConfig["on_demand_percentage_above_base_capacity"],
						"SpotInstancePools":                   costOptimizedConfig["spot_instance_pools"],
					}
				}

				action = "CreateNodeGroup"
				err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
					response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, createNodeGroupRequest, &runtime)
					if err != nil {
						if NeedRetry(err) {
							wait()
							return resource.RetryableError(err)
						}
						return resource.NonRetryableError(err)
					}
					return nil
				})
				addDebug(action, response, createNodeGroupRequest)
				if err != nil {
					return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
				}

				listNodeGroupsRequest := map[string]interface{}{
					"ClusterId":      d.Id(),
					"NodeGroupNames": []string{nodeGroupName},
					"RegionId":       client.RegionId,
				}
				action = "ListNodeGroups"
				err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
					response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, listNodeGroupsRequest, &runtime)
					if err != nil {
						if NeedRetry(err) {
							wait()
							return resource.RetryableError(err)
						}
						return resource.NonRetryableError(err)
					}
					return nil
				})
				addDebug(action, response, listNodeGroupsRequest)
				if err != nil {
					return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
				}
				resp, err := jsonpath.Get("$.NodeGroups", response)
				if err != nil {
					return WrapErrorf(err, FailedGetAttributeMsg, d.Id(), "$.NodeGroups", response)
				}

				if len(resp.([]interface{})) == 0 {
					continue
				}

				nodeGroupId := resp.([]interface{})[0].(map[string]interface{})["NodeGroupId"].(string)

				newNodeCount := formatInt(newNodeGroup["node_count"])
				if newNodeCount > 0 {
					increaseNodesGroup := map[string]interface{}{}
					increaseNodesGroup["RegionId"] = client.RegionId
					increaseNodesGroup["ClusterId"] = d.Id()
					increaseNodesGroup["NodeGroupId"] = nodeGroupId
					increaseNodesGroup["IncreaseNodeCount"] = newNodeCount
					increaseNodesGroup["AutoPayOrder"] = newNodeGroup["auto_pay_order"]
					if "Subscription" == newNodeGroup["payment_type"].(string) {
						subscriptionConfig := newNodeGroup["subscription_config"].(*schema.Set).List()
						if len(subscriptionConfig) == 1 {
							configMap := subscriptionConfig[0].(map[string]interface{})
							increaseNodesGroup["PaymentDuration"] = configMap["payment_duration"]
							increaseNodesGroup["PaymentDurationUnit"] = configMap["payment_duration_unit"]
						}
					}
					increaseNodesGroups = append(increaseNodesGroups, increaseNodesGroup)
				}
			}
		}

		var deleteNodeGroups []map[string]interface{}
		for nodeGroupName, oldNodeGroup := range oldNodeGroupMap { // Decrease nodes and delete empty nodeGroup
			if _, ok := newNodeGroupMap[nodeGroupName]; !ok {
				if "TASK" != oldNodeGroup["node_group_type"].(string) {
					return WrapError(Error("EMR cluster can only decrease the node group type of [TASK]."))
				}
				oldNodeCount := formatInt(oldNodeGroup["RunningNodeCount"])
				if oldNodeCount > 0 {
					decreaseNodesGroup := map[string]interface{}{
						"ClusterId":         d.Id(),
						"RegionId":          client.RegionId,
						"DecreaseNodeCount": oldNodeCount,
						"NodeGroupId":       oldNodeGroup["NodeGroupId"],
					}
					decreaseNodesGroups = append(decreaseNodesGroups, decreaseNodesGroup)
				}
				deleteNodeGroups = append(deleteNodeGroups, map[string]interface{}{
					"ClusterId":   d.Id(),
					"NodeGroupId": oldNodeGroup["NodeGroupId"],
					"RegionId":    client.RegionId,
				})
			}
		}

		for _, increaseNodesGroupRequest := range increaseNodesGroups {
			action := "IncreaseNodes"
			runtime := util.RuntimeOptions{}
			runtime.SetAutoretry(true)
			wait := incrementalWait(3*time.Second, 5*time.Second)
			err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
				response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"),
					StringPointer("2021-03-20"), StringPointer("AK"), nil, increaseNodesGroupRequest, &runtime)
				if err != nil {
					if NeedRetry(err) {
						wait()
						return resource.RetryableError(err)
					}
					return resource.NonRetryableError(err)
				}
				return nil
			})
			addDebug(action, response, increaseNodesGroupRequest)
			if err != nil {
				return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
			}
		}

		for _, decreaseNodesGroupRequest := range decreaseNodesGroups {
			action := "DecreaseNodes"
			runtime := util.RuntimeOptions{}
			runtime.SetAutoretry(true)
			wait := incrementalWait(3*time.Second, 5*time.Second)
			err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
				response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"),
					StringPointer("2021-03-20"), StringPointer("AK"), nil, decreaseNodesGroupRequest, &runtime)
				if err != nil {
					if NeedRetry(err) {
						wait()
						return resource.RetryableError(err)
					}
					return resource.NonRetryableError(err)
				}
				return nil
			})
			addDebug(action, response, decreaseNodesGroupRequest)
			if err != nil {
				return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
			}
		}

		for _, deleteNodeGroupRequest := range deleteNodeGroups {
			action := "DeleteNodeGroup"
			runtime := util.RuntimeOptions{}
			runtime.SetAutoretry(true)
			wait := incrementalWait(3*time.Second, 5*time.Second)
			err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
				response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"),
					StringPointer("2021-03-20"), StringPointer("AK"), nil, deleteNodeGroupRequest, &runtime)
				if err != nil {
					if NeedRetry(err) {
						wait()
						return resource.RetryableError(err)
					}
					return resource.NonRetryableError(err)
				}
				return nil
			})
			addDebug(action, response, deleteNodeGroupRequest)
			if err != nil {
				return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
			}
		}

		d.SetPartial("host_group")
	}

	if d.HasChange("applications") {
		_, newApplications := d.GetChange("applications")
		newApplicationsMap := map[string]struct{}{}
		for _, app := range newApplications.([]interface{}) {
			newApplicationsMap[app.(string)] = struct{}{}
		}
		action := "ListApplications"
		listApplicationsRequest := map[string]interface{}{
			"ClusterId": d.Id(),
			"RegionId":  client.RegionId,
		}
		runtime := util.RuntimeOptions{}
		runtime.SetAutoretry(true)
		wait := incrementalWait(3*time.Second, 5*time.Second)
		err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
			response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, listApplicationsRequest, &runtime)
			if err != nil {
				if NeedRetry(err) {
					wait()
					return resource.RetryableError(err)
				}
				return resource.NonRetryableError(err)
			}
			return nil
		})
		addDebug(action, response, listApplicationsRequest)
		if err != nil {
			return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
		}
		resp, err := jsonpath.Get("$.Applications", response)
		if err != nil {
			return WrapErrorf(err, FailedGetAttributeMsg, d.Id(), "$.Applications", response)
		}
		currApplicationsMap := map[string]struct{}{}
		for _, appItem := range resp.([]interface{}) {
			applicationName := appItem.(map[string]interface{})["ApplicationName"].(string)
			currApplicationsMap[strings.ToUpper(applicationName)] = struct{}{}
		}
		var newAddApplications []map[string]interface{}
		for newApp := range newApplicationsMap {
			if _, exists := currApplicationsMap[newApp]; !exists {
				newAddApplications = append(newAddApplications, map[string]interface{}{
					"ApplicationName": newApp,
				})
			}
		}
		if len(newAddApplications) > 0 {
			action = "AddApplications"
			addApplicationsRequest := map[string]interface{}{
				"ClusterId":    d.Id(),
				"RegionId":     client.RegionId,
				"Applications": newAddApplications,
			}
			runtime = util.RuntimeOptions{}
			runtime.SetAutoretry(true)
			wait = incrementalWait(3*time.Second, 5*time.Second)
			err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
				response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, addApplicationsRequest, &runtime)
				if err != nil {
					if NeedRetry(err) {
						wait()
						return resource.RetryableError(err)
					}
					return resource.NonRetryableError(err)
				}
				return nil
			})
			addDebug(action, response, addApplicationsRequest)
			if err != nil {
				return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
			}
		}
	}

	if d.HasChange("application_configs") {
		action := "UpdateApplicationConfigs"
		var updateApplicationConfigs []map[string]interface{}

		oldConfigs, newConfigs := d.GetChange("application_configs")
		genConfigKeyFunc := func(params ...interface{}) string {
			var keys []string
			for _, key := range params {
				keys = append(keys, key.(string))
			}
			return strings.Join(keys, "@")
		}
		configMapConverter := func(source []interface{}) map[string]map[string]interface{} {
			resultMap := map[string]map[string]interface{}{}
			for _, item := range source {
				if m, ok := item.(map[string]interface{}); ok {
					var appName string
					var fileName string
					var itemKey string
					if v, exists := m["application_name"]; !exists {
						continue
					} else {
						appName = v.(string)
					}
					if v, exists := m["config_file_name"]; !exists {
						continue
					} else {
						fileName = v.(string)
					}
					if v, exists := m["config_item_key"]; !exists {
						continue
					} else {
						itemKey = v.(string)
					}
					resultMap[genConfigKeyFunc(appName, fileName, itemKey)] = m
				}
			}
			return resultMap
		}

		oldKey2ConfigMap := configMapConverter(oldConfigs.(*schema.Set).List())
		for _, newConfigItem := range newConfigs.(*schema.Set).List() {
			configNeedUpdate := false
			configAction := "UPDATE"
			newConfigMap := newConfigItem.(map[string]interface{})
			newConfigKey := genConfigKeyFunc(newConfigMap["application_name"],
				newConfigMap["config_file_name"], newConfigMap["config_item_key"])

			if oldConfigMap, exists := oldKey2ConfigMap[newConfigKey]; !exists {
				configAction = "ADD"
				configNeedUpdate = true
			} else if oldConfigMap["config_item_value"] != newConfigMap["config_item_value"] {
				configAction = "UPDATE"
				configNeedUpdate = true
			}
			if configNeedUpdate {
				updateApplicationConfigs = append(updateApplicationConfigs, map[string]interface{}{
					"ClusterId":       d.Id(),
					"RegionId":        client.RegionId,
					"ApplicationName": newConfigMap["application_name"],
					"Description":     newConfigMap["config_description"],
					"RefreshConfig":   true,
					"ApplicationConfigs": []map[string]interface{}{
						{
							"ConfigFileName":    newConfigMap["config_file_name"],
							"ConfigItemKey":     newConfigMap["config_item_key"],
							"ConfigItemValue":   newConfigMap["config_item_value"],
							"ConfigDescription": newConfigMap["config_description"],
							"ConfigScope":       newConfigMap["config_scope"],
							"ConfigAction":      configAction,
						},
					},
				})
			}
		}

		newKey2Configmap := configMapConverter(newConfigs.(*schema.Set).List())
		for _, oldConfigItem := range oldConfigs.(*schema.Set).List() {
			oldConfigMap := oldConfigItem.(map[string]interface{})
			oldConfigKey := genConfigKeyFunc(oldConfigMap["application_name"],
				oldConfigMap["config_file_name"], oldConfigMap["config_item_key"])

			if _, exists := newKey2Configmap[oldConfigKey]; !exists {
				updateApplicationConfigs = append(updateApplicationConfigs, map[string]interface{}{
					"ClusterId":       d.Id(),
					"RegionId":        client.RegionId,
					"ApplicationName": oldConfigMap["application_name"],
					"RefreshConfig":   true,
					"ApplicationConfigs": []map[string]interface{}{
						{
							"ConfigFileName":    oldConfigMap["config_file_name"],
							"ConfigItemKey":     oldConfigMap["config_item_key"],
							"ConfigItemValue":   oldConfigMap["config_item_value"],
							"ConfigDescription": oldConfigMap["config_description"],
							"ConfigScope":       oldConfigMap["config_scope"],
							"ConfigAction":      "DELETE",
						},
					},
				})
			}
		}

		for _, updateApplicationConfigsRequest := range updateApplicationConfigs {
			runtime := util.RuntimeOptions{}
			runtime.SetAutoretry(true)
			wait := incrementalWait(3*time.Second, 5*time.Second)
			err = resource.Retry(d.Timeout(schema.TimeoutUpdate), func() *resource.RetryError {
				response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"),
					StringPointer("2021-03-20"), StringPointer("AK"), nil, updateApplicationConfigsRequest, &runtime)
				if err != nil {
					if NeedRetry(err) {
						wait()
						return resource.RetryableError(err)
					}
					return resource.NonRetryableError(err)
				}
				return nil
			})
			addDebug(action, response, updateApplicationConfigsRequest)
			if err != nil {
				return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
			}
		}
		d.SetPartial("application_configs")
	}

	d.Partial(false)

	return nil
}

func resourceAlicloudEmrClusterDeleteNew(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*connectivity.AliyunClient)
	emrService := EmrService{client}
	var response map[string]interface{}
	conn, err := client.NewEmrClient()
	if err != nil {
		return WrapError(err)
	}
	action := "GetCluster"
	request := map[string]interface{}{
		"ClusterId": d.Id(),
		"RegionId":  client.RegionId,
	}
	runtime := util.RuntimeOptions{}
	runtime.SetAutoretry(true)
	wait := incrementalWait(3*time.Second, 5*time.Second)
	err = resource.Retry(d.Timeout(schema.TimeoutDelete), func() *resource.RetryError {
		response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, request, &runtime)
		if err != nil {
			if NeedRetry(err) {
				wait()
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	addDebug(action, response, request)
	if err != nil {
		return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
	}
	v, err := jsonpath.Get("$.Cluster", response)
	if err != nil {
		return WrapErrorf(err, FailedGetAttributeMsg, d.Id(), "$.Cluster", response)
	}
	if v.(map[string]interface{})["PaymentType"] == "Subscription" {
		return WrapError(Error("EMR 'Subscription' cluster can not delete, please release all running nodes with current cluster."))
	}

	action = "DeleteCluster"
	err = resource.Retry(d.Timeout(schema.TimeoutDelete), func() *resource.RetryError {
		response, err = conn.DoRequest(StringPointer(action), nil, StringPointer("POST"), StringPointer("2021-03-20"), StringPointer("AK"), nil, request, &runtime)
		if err != nil {
			if NeedRetry(err) {
				wait()
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	addDebug(action, response, request)
	if err != nil {
		return WrapErrorf(err, DefaultErrorMsg, d.Id(), action, AlibabaCloudSdkGoERROR)
	}

	stateConf := BuildStateConf([]string{"TERMINATING"}, []string{}, d.Timeout(schema.TimeoutDelete), 1*time.Millisecond, emrService.EmrClusterNewStateRefreshFunc(d.Id(), []string{"TERMINATE_FAILED"}))
	stateConf.PollInterval = 5 * time.Second
	if _, err := stateConf.WaitForState(); err != nil {
		return WrapErrorf(err, IdMsg, d.Id())
	}

	return WrapError(emrService.WaitForEmrClusterNew(d.Id(), Deleted, DefaultTimeoutMedium))
}
