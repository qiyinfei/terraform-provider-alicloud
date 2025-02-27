package alicloud

import (
	"fmt"
	"regexp"
	"time"

	"github.com/PaesslerAG/jsonpath"
	"github.com/aliyun/terraform-provider-alicloud/alicloud/connectivity"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
)

func dataSourceAlicloudArmsAlertRobots() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAlicloudArmsAlertRobotsRead,
		Schema: map[string]*schema.Schema{
			"ids": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Computed: true,
			},
			"name_regex": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.ValidateRegexp,
				ForceNew:     true,
			},
			"names": {
				Type:     schema.TypeList,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Computed: true,
			},
			"alert_robot_name": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"robot_type": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"output_file": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"robots": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"robot_id": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"robot_name": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"robot_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"robot_addr": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"daily_noc": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"daily_noc_time": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"create_time": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
	}
}

func dataSourceAlicloudArmsAlertRobotsRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*connectivity.AliyunClient)

	action := "DescribeIMRobots"
	request := make(map[string]interface{})
	if v, ok := d.GetOk("alert_robot_name"); ok {
		request["RobotName"] = v
	}
	request["Page"] = 1
	request["Size"] = PageSizeXLarge
	var objects []map[string]interface{}
	var alertContactRobotNameRegex *regexp.Regexp
	if v, ok := d.GetOk("name_regex"); ok {
		r, err := regexp.Compile(v.(string))
		if err != nil {
			return WrapError(err)
		}
		alertContactRobotNameRegex = r
	}

	idsMap := make(map[string]string)
	if v, ok := d.GetOk("ids"); ok {
		for _, vv := range v.([]interface{}) {
			if vv == nil {
				continue
			}
			idsMap[vv.(string)] = vv.(string)
		}
	}
	var response map[string]interface{}
	var err error
	wait := incrementalWait(3*time.Second, 3*time.Second)
	err = resource.Retry(5*time.Minute, func() *resource.RetryError {
		response, err = client.RpcPost("ARMS", "2019-08-08", action, nil, request, true)
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
		return WrapErrorf(err, DataDefaultErrorMsg, "alicloud_arms_alert_robots", action, AlibabaCloudSdkGoERROR)
	}
	resp, err := jsonpath.Get("$.PageBean.AlertIMRobots", response)
	if err != nil {
		return WrapErrorf(err, FailedGetAttributeMsg, action, "$.PageBean.AlertIMRobots", response)
	}
	result, _ := resp.([]interface{})
	for _, v := range result {
		item := v.(map[string]interface{})
		if alertContactRobotNameRegex != nil && !alertContactRobotNameRegex.MatchString(fmt.Sprint(item["RobotName"])) {
			continue
		}
		if len(idsMap) > 0 {
			if _, ok := idsMap[fmt.Sprint(item["RobotId"])]; !ok {
				continue
			}
		}
		objects = append(objects, item)
	}
	ids := make([]string, 0)
	names := make([]interface{}, 0)
	s := make([]map[string]interface{}, 0)
	for _, object := range objects {
		mapping := map[string]interface{}{
			"id":             fmt.Sprint(object["RobotId"]),
			"robot_type":     object["Type"],
			"robot_id":       fmt.Sprint(object["RobotId"]),
			"robot_name":     object["RobotName"],
			"robot_addr":     object["RobotAddr"],
			"daily_noc":      fmt.Sprint(object["DailyNoc"]),
			"daily_noc_time": object["DailyNocTime"],
			"create_time":    fmt.Sprint(object["CreateTime"]),
		}
		ids = append(ids, fmt.Sprint(mapping["id"]))
		names = append(names, object["RobotName"])
		s = append(s, mapping)
	}

	d.SetId(dataResourceIdHash(ids))
	if err := d.Set("ids", ids); err != nil {
		return WrapError(err)
	}

	if err := d.Set("names", names); err != nil {
		return WrapError(err)
	}

	if err := d.Set("robots", s); err != nil {
		return WrapError(err)
	}
	if output, ok := d.GetOk("output_file"); ok && output.(string) != "" {
		writeToFile(output.(string), s)
	}

	return nil
}
