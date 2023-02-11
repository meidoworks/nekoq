package discovery

var _nameAllowed [256]bool

func init() {
	for i := 0; i < len(_nameAllowed); i++ {
		if i >= 'A' && i <= 'Z' {
			_nameAllowed[i] = true
		} else if i >= 'a' && i <= 'z' {
			_nameAllowed[i] = true
		} else if i >= '0' && i <= '9' {
			_nameAllowed[i] = true
		} else {
			switch i {
			case '.':
				fallthrough
			case '-':
				fallthrough
			case '_':
				fallthrough
			case '/':
				fallthrough
			case ':':
				fallthrough
			case '\\':
				_nameAllowed[i] = true
			default:
				_nameAllowed[i] = false
			}
		}
	}
}

func validateName(s string) bool {
	//matched, err := regexp.MatchString(`^[A-Za-z0-9.\-_/:\\]+$`, s)
	for _, v := range []byte(s) {
		if !_nameAllowed[v] {
			return false
		}
	}
	return true
}

func validateServiceName(s string) bool {
	return validateName(s)
}

func validateAreaName(s string) bool {
	return validateName(s)
}

func validateNodeId(s string) bool {
	return validateName(s)
}
