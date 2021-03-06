package drds

//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//
// Code generated by Alibaba Cloud SDK Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/responses"
)

// CancleDDLTask invokes the drds.CancleDDLTask API synchronously
func (client *Client) CancleDDLTask(request *CancleDDLTaskRequest) (response *CancleDDLTaskResponse, err error) {
	response = CreateCancleDDLTaskResponse()
	err = client.DoAction(request, response)
	return
}

// CancleDDLTaskWithChan invokes the drds.CancleDDLTask API asynchronously
func (client *Client) CancleDDLTaskWithChan(request *CancleDDLTaskRequest) (<-chan *CancleDDLTaskResponse, <-chan error) {
	responseChan := make(chan *CancleDDLTaskResponse, 1)
	errChan := make(chan error, 1)
	err := client.AddAsyncTask(func() {
		defer close(responseChan)
		defer close(errChan)
		response, err := client.CancleDDLTask(request)
		if err != nil {
			errChan <- err
		} else {
			responseChan <- response
		}
	})
	if err != nil {
		errChan <- err
		close(responseChan)
		close(errChan)
	}
	return responseChan, errChan
}

// CancleDDLTaskWithCallback invokes the drds.CancleDDLTask API asynchronously
func (client *Client) CancleDDLTaskWithCallback(request *CancleDDLTaskRequest, callback func(response *CancleDDLTaskResponse, err error)) <-chan int {
	result := make(chan int, 1)
	err := client.AddAsyncTask(func() {
		var response *CancleDDLTaskResponse
		var err error
		defer close(result)
		response, err = client.CancleDDLTask(request)
		callback(response, err)
		result <- 1
	})
	if err != nil {
		defer close(result)
		callback(nil, err)
		result <- 0
	}
	return result
}

// CancleDDLTaskRequest is the request struct for api CancleDDLTask
type CancleDDLTaskRequest struct {
	*requests.RpcRequest
	DrdsInstanceId string `position:"Query" name:"DrdsInstanceId"`
	DbName         string `position:"Query" name:"DbName"`
	TaskId         string `position:"Query" name:"TaskId"`
}

// CancleDDLTaskResponse is the response struct for api CancleDDLTask
type CancleDDLTaskResponse struct {
	*responses.BaseResponse
	RequestId string `json:"RequestId" xml:"RequestId"`
}

// CreateCancleDDLTaskRequest creates a request to invoke CancleDDLTask API
func CreateCancleDDLTaskRequest() (request *CancleDDLTaskRequest) {
	request = &CancleDDLTaskRequest{
		RpcRequest: &requests.RpcRequest{},
	}
	request.InitWithApiInfo("Drds", "2015-04-13", "CancleDDLTask", "Drds", "openAPI")
	request.Method = requests.POST
	return
}

// CreateCancleDDLTaskResponse creates a response to parse from CancleDDLTask response
func CreateCancleDDLTaskResponse() (response *CancleDDLTaskResponse) {
	response = &CancleDDLTaskResponse{
		BaseResponse: &responses.BaseResponse{},
	}
	return
}
