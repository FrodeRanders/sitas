% -----------------------------------------------------------------------------
% LaTeX class for the sitas system manual.
%
% This is intentionally small and local to docs/system. It follows the same
% manual shape as the ipto system documentation: A4 book layout, compact
% headings, source listings, hyperlinks, a title page, and reusable helpers for
% paths, shell commands, and notes.
% -----------------------------------------------------------------------------

\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{sitasdoc}[2026/05/16 Class for sitas technical documentation]

\DeclareOption*{\PassOptionsToClass{\CurrentOption}{book}}
\ProcessOptions\relax

\LoadClass[oneside,a4paper,12pt]{book}

\RequirePackage[total={165mm,262mm},top=15mm,left=24mm,bindingoffset=0mm,includefoot,includehead,headheight=14.5pt]{geometry}
\RequirePackage[T1]{fontenc}
\RequirePackage[utf8]{inputenc}
\RequirePackage{helvet}
\RequirePackage{times}
\RequirePackage{longtable}
\RequirePackage{array}
\RequirePackage{enumitem}
\RequirePackage[table]{xcolor}
\RequirePackage{graphicx}
\RequirePackage{fancyhdr}
\RequirePackage{titlesec}
\RequirePackage{titletoc}
\RequirePackage[nottoc]{tocbibind}
\RequirePackage[final]{listings}
\RequirePackage[font={small,it},margin=10pt,justification=centerlast,position=below]{caption}
\RequirePackage{url}
\RequirePackage[pdftex,colorlinks=true,linkcolor=sitasInk,urlcolor=sitasAccent,citecolor=sitasAccent]{hyperref}

\definecolor{sitasInk}{HTML}{0B1220}
\definecolor{sitasMuted}{HTML}{334155}
\definecolor{sitasAccent}{HTML}{0E7490}
\definecolor{sitasCodeBackground}{HTML}{F2F2F2}
\definecolor{sitasCodeComment}{HTML}{166534}
\definecolor{sitasCodeKeyword}{HTML}{9F1239}
\definecolor{sitasCodeNumber}{HTML}{64748B}

\setlength{\parindent}{0pt}
\setlength{\parskip}{0.65\baselineskip plus 0.1\baselineskip minus 0.05\baselineskip}
\setlength{\emergencystretch}{2em}
\setlist[itemize]{itemsep=0.15\baselineskip,topsep=0.3\baselineskip,parsep=0pt,partopsep=0pt}
\setlist[enumerate]{itemsep=0.15\baselineskip,topsep=0.3\baselineskip,parsep=0pt,partopsep=0pt}
\setlist[description]{style=unboxed,leftmargin=0pt,labelsep=0.5em,itemsep=0.1\baselineskip,topsep=0.2\baselineskip,parsep=0pt,partopsep=0pt}

\setcounter{secnumdepth}{3}
\setcounter{tocdepth}{2}

\lstdefinestyle{sitasstyle}{
    backgroundcolor=\color{sitasCodeBackground},
    commentstyle=\color{sitasCodeComment},
    keywordstyle=\color{sitasCodeKeyword},
    numberstyle=\tiny\color{sitasCodeNumber},
    stringstyle=\color{sitasCodeKeyword},
    basicstyle=\footnotesize\ttfamily,
    breakatwhitespace=false,
    breaklines=true,
    captionpos=b,
    keepspaces=true,
    numbers=left,
    showspaces=false,
    showstringspaces=false,
    showtabs=false
}
\lstset{style=sitasstyle,upquote=true}

\newenvironment{compressed_itemize}{
    \vspace{-0.2cm}\begin{itemize}
    \setlength{\itemsep}{0pt}
    \setlength{\parskip}{0pt}
    \setlength{\parsep}{0pt}
}
{
    \end{itemize}
}

\newenvironment{notabene}[1][Note]{
    \begin{quote}
    \small
    \noindent\textbf{#1}\quad
}
{
    \end{quote}
}

\newcommand{\filepath}[1]{%
    \begingroup
    \def\UrlBreaks{\do\/\do\.\do\_\do\-}%
    \nolinkurl{#1}%
    \endgroup
}

\newcommand{\shellcmd}[1]{%
    \begingroup
    \def\UrlBreaks{\do\/\do\.\do\_\do\-\do\ }%
    \nolinkurl{#1}%
    \endgroup
}

\newcommand{\literalbreak}[1]{%
    \begingroup
    \def\UrlBreaks{\do\.\do\_\do\-}%
    \nolinkurl{#1}%
    \endgroup
}

\newcommand{\mychaptertitlefont}{\fontseries{m}\fontsize{20}{24}\selectfont}

\makeatletter
\def\title#1{\gdef\@thetitle{#1}}
\def\subtitle#1{\gdef\@thesubtitle{#1}}
\def\editors#1{\gdef\@theeditors{#1}}

\def\@thetitle{}
\def\@thesubtitle{}
\def\@theeditors{}

\renewcommand{\maketitle}{%
    \thispagestyle{empty}
    \setlength{\parindent}{0mm}

    \vspace*{18mm}
    \begin{center}
        {\Huge\textsc{\@thetitle{}}}

        \vspace*{12mm}
        \rule{0.78\linewidth}{0.5pt}

        \vspace*{24mm}
        {\huge\textsc{\@thesubtitle{}}}

        \vspace*{36mm}
        {\Large\@theeditors{}}

        \vfill
        {\today}
    \end{center}
}
\makeatother

\pagestyle{fancy}
\fancyhf{}
\lhead{\nouppercase{\leftmark}}
\rhead{\thepage}
\lfoot{sitas system manual}
\rfoot{\today}
\renewcommand{\headrulewidth}{0.4pt}
\renewcommand{\footrulewidth}{0.4pt}

\titleformat{name=\chapter,numberless=false}[display]
{\large}
{\vspace{-12mm}}
{1ex}{\mychaptertitlefont \thechapter \hspace{4mm}}

\titleformat{name=\chapter,numberless=true}[display]
{\large}
{\vspace{-12mm}}
{1ex}{\mychaptertitlefont}

\titlespacing{\chapter}{0mm}{*0}{*1}

\titlecontents{chapter}[0pt]{\addvspace{4mm}\rmfamily\bfseries\Large}
{\thecontentslabel\quad}{}{\titlerule*[5pt]{.}\contentspage}

\titlecontents{section}[4mm]{\addvspace{2pt}\rmfamily\bfseries\large}
{\thecontentslabel\quad}{}{\titlerule*[5pt]{.}\contentspage}

\titlecontents{subsection}[9mm]{\addvspace{2pt}\rmfamily}
{\thecontentslabel\quad}{}{\titlerule*[5pt]{.}\contentspage}
