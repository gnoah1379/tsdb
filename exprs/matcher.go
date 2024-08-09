package exprs

type LabelOperation string

const (
	LabelEq        LabelOperation = "="
	LabelNe        LabelOperation = "!="
	LabelIn        LabelOperation = "in"
	LabelNotIn     LabelOperation = "not in"
	LabelExists    LabelOperation = "exists"
	LabelNotExists LabelOperation = "not exists"
)

type LabelExpr struct {
	Label  string
	Op     LabelOperation
	Values []string
}

func Eq(label string, value string) LabelExpr {
	return LabelExpr{
		Label:  label,
		Op:     LabelEq,
		Values: []string{value},
	}
}

func Ne(label string, value string) LabelExpr {
	return LabelExpr{
		Label:  label,
		Op:     LabelNe,
		Values: []string{value},
	}
}

func In(label string, values ...string) LabelExpr {
	return LabelExpr{
		Label:  label,
		Op:     LabelIn,
		Values: values,
	}
}

func NotIn(label string, values ...string) LabelExpr {
	return LabelExpr{
		Label:  label,
		Op:     LabelNotIn,
		Values: values,
	}
}

func Exists(label string) LabelExpr {
	return LabelExpr{
		Label: label,
		Op:    LabelExists,
	}
}

func NotExists(label string) LabelExpr {
	return LabelExpr{
		Label: label,
		Op:    LabelNotExists,
	}
}

func MatchLabels(filters []LabelExpr, labels map[string]string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, expr := range filters {
		val, exist := labels[expr.Label]
		switch expr.Op {
		case LabelExists:
			if !exist {
				return false
			}
		case LabelNotExists:
			if exist {
				return false
			}
		case LabelEq:
			if val != expr.Values[0] {
				return false
			}
		case LabelNe:
			if val == expr.Values[0] {
				return false
			}
		case LabelIn:
			found := false
			for _, v := range expr.Values {
				if val == v {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		case LabelNotIn:
			found := false
			for _, v := range expr.Values {
				if val == v {
					found = true
					break
				}
			}
			if found {
				return false
			}
		}
	}
	return true
}
