package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

type Grade string

const (
	A Grade = "A"
	B Grade = "B"
	C Grade = "C"
	F Grade = "F"
)

type student struct {
	firstName, lastName, university                string
	test1Score, test2Score, test3Score, test4Score int
}

type studentStat struct {
	student
	finalScore float32
	grade      Grade
}

func (stu student) String() string {
	return fmt.Sprintf("Student's name is %v %v and university is %v", stu.firstName, stu.lastName, stu.university)
}

func parseCSV(filePath string) []student {

	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file"+filePath, err)
	}
	defer f.Close()

	Reader := csv.NewReader(f)
	records, err := Reader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file"+filePath, err)
	}

	students := []student{}

	for key, record := range records {
		if key == 0 {
			continue
		}
		test1Score, _ := strconv.Atoi(record[3])
		test2Score, _ := strconv.Atoi(record[4])
		test3Score, _ := strconv.Atoi(record[5])
		test4Score, _ := strconv.Atoi(record[6])
		student := student{
			firstName:  record[0],
			lastName:   record[1],
			university: record[2],
			test1Score: test1Score,
			test2Score: test2Score,
			test3Score: test3Score,
			test4Score: test4Score,
		}
		students = append(students, student)
	}
	return students
}

func calculateGrade(students []student) []studentStat {
	studentStats := []studentStat{}
	for _, student := range students {
		finalScore := (float32(student.test1Score) + float32(student.test2Score) + float32(student.test3Score) + float32(student.test4Score)) / 4.0
		grade := Grade("")
		switch {
		case finalScore < 35:
			grade = F
		case finalScore >= 35 && finalScore < 50:
			grade = C
		case finalScore >= 50 && finalScore < 70:
			grade = B
		case finalScore >= 70:
			grade = A
		}
		studentStat := studentStat{
			student:    student,
			finalScore: float32(finalScore),
			grade:      grade,
		}
		studentStats = append(studentStats, studentStat)
	}
	return studentStats
}

func findOverallTopper(gradedStudents []studentStat) studentStat {
	topperStudentStrat := gradedStudents[0]
	for _, stat := range gradedStudents[1:] {
		topperStudent := topperStudentStrat.student
		topperStudentScores := topperStudent.test1Score + topperStudent.test2Score + topperStudent.test3Score + topperStudent.test4Score

		currentStudent := stat.student
		currentStudentScores := currentStudent.test1Score + currentStudent.test2Score + currentStudent.test3Score + currentStudent.test4Score

		if currentStudentScores > topperStudentScores {
			topperStudentStrat = stat
		}
	}
	return topperStudentStrat
}

func findTopperPerUniversity(gs []studentStat) map[string]studentStat {
	studentsByUniversity := make(map[string][]studentStat)
	topperPerUniversity := make(map[string]studentStat)

	for _, stat := range gs {
		_, ok := studentsByUniversity[stat.student.university]
		if ok {
			studentsByUniversity[stat.student.university] = append(studentsByUniversity[stat.student.university], stat)
		} else {
			studentsByUniversity[stat.student.university] = append([]studentStat{}, stat)
		}
	}

	for uni, students := range studentsByUniversity {
		topperPerUniversity[uni] = findOverallTopper(students)
	}

	return topperPerUniversity
}
