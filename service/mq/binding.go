package mq

func validateBindingKey(key string) bool {
	//TODO validate binding key in binding flow
	return true
}

func validateMatchingBindingKey(key string) bool {
	return ValidateNameForBrokerMechanisms(key)
}

func matchBindingKey(bindingKey, matchingBindingKey string) bool {
	//TODO support wildcard matching
	return bindingKey == matchingBindingKey
}
