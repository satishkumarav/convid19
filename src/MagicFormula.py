def mystery(A, l, r):
    if l >= r: return -1
    m = int((l + r) / 2)
    print("M : ", m)
    if A[m] == m:
        return m
    else:
        if A[m] < m:
            l1 = m + 1
            return mystery(A, l=l1, r=r)
        else:
            return mystery(A, l=l, r=(m - 1))


A = [0, 1, 4, 5, 9, 5]
left = 0
right = 6
print(A)

print("Return : ", mystery(A, l=int(left), r=int(right)))
