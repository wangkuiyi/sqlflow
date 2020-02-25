// Copyright 2020 The SQLFlow Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/language"
	"sqlflow.org/sqlflow/pkg/sql/codegen/tensorflow"
	"sqlflow.org/sqlflow/pkg/sql/codegen/xgboost"
)

const (
	preemble = map[language.Tag]string{
		language.English: `# Model Parameters

Users can use the WITH-clause of SQLFlow extended syntax to specify
hyperparameters of models. This document parameters that can appear
in WITH-clause.
`,
		language.Chinese: `# 模型参数

用户可以用 WITH 从句指定模型的超参数。本文档列出各种模型可以接受的超参数。
`}
)

func main() {
	l := flag.String("lang", "en", "language of generated documents")
	flag.Parse()
	lang := language.MustParse(*l)

	fmt.Print(premepble[lang])
	docGenFunc := []func() string{
		xgboost.DocGenInMarkdown,
		tensorflow.DocGenInMarkdown,
	}

	section := regexp.MustCompile(`^#{1,5} `)
	for _, f := range docGenFunc {
		lines := strings.Split(f(lang), "\n")
		for i := range lines {
			// convert title -> section, section -> subsection
			if section.MatchString(lines[i]) {
				lines[i] = "#" + lines[i]
			}
			fmt.Println(lines[i])
		}
	}
}
