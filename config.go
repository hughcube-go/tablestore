package tablestore

type Config struct {
	EndPoint        string
	InstanceName    string
	AccessKeyId     string
	AccessKeySecret string
	Options         []ClientOption
}
