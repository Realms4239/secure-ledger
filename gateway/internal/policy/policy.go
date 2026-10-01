package policy

func Authorized(subject, fromAccount string) bool { return subject == fromAccount }
